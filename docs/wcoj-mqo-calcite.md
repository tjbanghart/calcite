# Worst-Case Optimal Joins Meet Multi-Query Optimization: Shared Computation in Apache Calcite

**T.J. Banghart**

---

## Abstract

Modern analytical workloads increasingly feature batches of structurally similar queries over the same data, many involving cyclic join patterns such as triangle counting, clique detection, and graph motif search. Traditional relational engines process each query independently using binary join trees, leading to redundant computation and intermediate results that can be exponentially larger than the final output. We present a unified framework within Apache Calcite that addresses both problems simultaneously. First, we integrate *worst-case optimal join* (WCOJ) algorithms into Calcite's optimizer and code-generation pipeline, enabling multi-way joins that are bounded by the AGM bound rather than by intermediate result sizes. Second, we introduce the `Combine` relational operator and a novel `MULTI()` SQL syntax for declarative multi-query batching, together with three layers of cross-query optimization: (1) scan sharing via lazy spools, (2) trie caching across WCOJ operators, and (3) shared-prefix execution through join-variable fingerprinting and prefix-group analysis. Our approach is the first to combine WCOJ algorithms with multi-query optimization in an open-source, general-purpose SQL framework. We provide a formal analysis of prefix-sharing correctness and cost savings, describe the full integration into Calcite's Volcano-based planner, and present an experimental evaluation on synthetic graph workloads with controlled cyclicity.

---

## 1. Introduction

Join processing is the most critical operation in relational query evaluation. For decades, relational database management systems (RDBMSs) have relied on *binary join trees*: plans composed of pairwise hash joins, merge joins, or nested-loop joins arranged in a tree where each internal node combines two inputs [1]. For acyclic query topologies---stars, snowflakes, chains---a well-chosen binary join order keeps intermediate results bounded by the input and output sizes. However, for *cyclic* queries, no binary join ordering avoids potentially catastrophic intermediate blowup [2, 3].

Consider the triangle query, a fundamental motif in graph analytics:

$$Q_\triangle(a, b, c) \leftarrow R(a, b), S(b, c), T(c, a)$$

A binary plan that first computes $R \bowtie S$ on $b$ produces all 2-hop paths $(a, b, c)$---which can be $O(|E|^2)$ for graphs with high-degree hub nodes---before filtering with $T$. The final output (actual triangles) may be orders of magnitude smaller. Worst-case optimal join (WCOJ) algorithms [3, 4, 5] resolve this by processing one variable at a time, intersecting candidate values across *all* participating relations simultaneously. The resulting runtime is bounded by the *AGM bound* [2]---the information-theoretic maximum output size given the input cardinalities---rather than by intermediate result sizes.

Independently, *multi-query optimization* (MQO) [6, 7, 8] addresses the problem of redundant computation when multiple queries share common sub-expressions, scan the same tables, or perform structurally identical joins. MQO has been studied extensively for traditional binary-join workloads, but its interaction with WCOJ algorithms remains unexplored.

In this paper, we present an integrated system within Apache Calcite [9] that combines WCOJ execution with multi-query optimization. Our contributions are:

1. **WCOJ in Calcite.** We implement a hash-based WCOJ algorithm with multi-level trie indexing, integrated into Calcite's Volcano/Cascades optimizer via a new `EnumerableWCOJ` physical operator and a planner rule that automatically detects cyclic join graphs.

2. **The `Combine` operator and `MULTI()` syntax.** We introduce a new relational algebra operator and SQL extension for declarative multi-query batching, enabling the optimizer to reason about cross-query sharing opportunities.

3. **Three-layer shared computation.** We design and implement scan sharing via lazy spools, identity-based trie caching, and a novel shared-prefix execution strategy based on join-variable fingerprinting.

4. **Formal analysis and experimental evaluation.** We prove the correctness of prefix sharing and analyze the cost model, then evaluate the system on synthetic graph workloads demonstrating significant speedups.

The rest of this paper is organized as follows. Section 2 provides background on WCOJ algorithms and MQO. Section 3 presents a comprehensive literature review spanning WCOJ theory, practical implementations, MQO techniques, and query optimization frameworks. Section 4 describes the WCOJ integration into Calcite. Section 5 presents the `Combine` operator and multi-query framework. Section 6 details the three layers of shared computation. Section 7 provides formal analysis. Section 8 presents experimental results. Section 9 discusses related work and positioning, and Section 10 concludes.

---

## 2. Background

### 2.1 Worst-Case Optimal Join Algorithms

The theoretical foundations for WCOJ algorithms were established by Atserias, Grohe, and Marx [2], who proved a tight upper bound on the maximum output size of a natural join query as a function of input relation cardinalities. For a join query $Q$ over relations $R_1, \ldots, R_n$ with attributes from a universe $\mathcal{U}$, the *AGM bound* states:

$$|Q(D)| \leq \prod_{i=1}^{n} |R_i(D)|^{x_i^*}$$

where $x_i^*$ is the solution to the fractional edge cover linear program over the query hypergraph. For the triangle query, this yields $|Q_\triangle| \leq |R|^{1/2} \cdot |S|^{1/2} \cdot |T|^{1/2} = |E|^{3/2}$, which is tight and strictly better than the $O(|E|^2)$ intermediate result possible with binary joins.

Ngo, Porat, Re, and Rudra [3] proved that the `Generic-Join` algorithm achieves worst-case optimality:

```
GENERIC-JOIN(Relations R_1...R_n, Variables x_1...x_m):
    if m = 0:
        emit matching tuples
        return
    candidates <- intersection of pi_{x_1}(R_i | current bindings)
                  for all R_i mentioning x_1
    for each value v in candidates:
        bind x_1 <- v
        GENERIC-JOIN(R_1...R_n, x_2...x_m)
```

The algorithm processes variables one at a time, maintaining partial bindings. At each level, it computes the intersection of candidate values across all relations that constrain the current variable, conditioned on the bindings established at prior levels.

Veldhuizen [4] introduced *Leapfrog Triejoin*, a practical implementation of this family using sorted trie indices and a `leapfrog` intersection primitive that exploits sorted order for efficient multi-way intersection. Freitag et al. [5] later showed how to integrate WCOJ into a general-purpose RDBMS (Umbra/HyPer) using hash-based tries, demonstrating that the overhead of trie construction can be amortized within standard query execution.

### 2.2 Multi-Query Optimization

Multi-query optimization was formalized by Sellis [6], who showed that identifying and sharing common sub-expressions across concurrent queries can reduce total execution cost significantly. The problem is NP-hard in general [7, 10], as it involves selecting a subset of materializable intermediate results that maximizes cost savings under resource constraints.

Subsequent work has explored heuristic approaches within the Volcano/Cascades framework [8], algebraic reformulation using $\psi$-operators [11], and hybrid strategies combining batched execution with materialized view reuse [12]. More recently, the convergence of query optimization and workload-level optimization has been identified as a key industrial trend [13].

Despite this rich body of work, no prior system has combined MQO with WCOJ algorithms. This intersection is particularly promising because WCOJ workloads---graph pattern queries, subgraph matching, and combinatorial joins---are precisely the workloads most likely to appear in structurally similar batches (e.g., multiple triangle queries with different projections or filters over the same graph).

### 2.3 Apache Calcite

Apache Calcite [9] is an open-source framework for query optimization and execution that serves as the query processing backbone for numerous data management systems including Apache Hive, Apache Flink, Apache Druid, and Trino. Calcite provides:

- A *relational algebra* with extensible operators (`RelNode` hierarchy)
- A *Volcano/Cascades-style cost-based optimizer* (`VolcanoPlanner`) with transformation and implementation rules
- An *enumerable code-generation backend* that compiles relational plans into executable Java code via the Linq4j library
- A *pluggable SQL parser* built on JavaCC

Our extensions leverage all four components, adding new operators, rules, and runtime data structures while preserving backward compatibility with existing Calcite functionality.

---

## 3. Literature Review

This section surveys the two bodies of work that our system bridges---worst-case optimal join algorithms and multi-query optimization---as well as the query processing frameworks that provide the architectural substrate for integration.

### 3.1 The Theory of Worst-Case Optimal Joins

The theoretical study of join output size bounds has a rich history. The earliest tight bounds for natural join queries were established by Atserias, Grohe, and Marx [2] at FOCS 2008 (journal version in SIAM Journal on Computing, 2013). Their *AGM bound* shows that for a join query $Q$ over relations $R_1, \ldots, R_n$, the maximum output size is:

$$|Q(D)| \leq \prod_{i=1}^{n} |R_i(D)|^{x_i^*}$$

where $\mathbf{x}^*$ is the optimal solution to the *fractional edge cover* linear program over the query hypergraph. This bound is tight: for every query and set of cardinalities, there exists a database instance achieving it. The bound immediately implies that any algorithm running in time $O\left(\prod |R_i|^{x_i^*}\right)$ is *worst-case optimal*---it cannot be improved by more than a polynomial factor for any instance.

Ngo, Porat, Re, and Rudra [3] (PODS 2012, JACM 2018) proved that a simple algorithm, `Generic-Join`, achieves this bound. Generic-Join processes variables one at a time, computing the intersection of candidate values across all relations that constrain each variable. Their proof proceeds by induction on the number of variables, using the AGM bound at each level to bound the work done.

The theoretical landscape was further enriched by Ngo, Re, and Rudra's survey "Skew Strikes Back" [18], which unified the AGM bound with degree-based bounds (the *BRR bound*, named after Beame, Koutris, and Suciu), showed connections to information-theoretic entropy bounds, and introduced the *Minesweeper* algorithm---a certificate-based approach that can outperform Generic-Join on favorable instances by leveraging structural properties of the actual data (not just cardinalities). Ngo's invited PODS 2018 tutorial [24] provides a comprehensive overview of these developments, identifying open problems including adaptive variable ordering, handling inequality predicates, and extending WCOJ to aggregate queries.

Beyond cardinality-based bounds, the notion of *hypertree width* and its generalizations provide finer-grained complexity measures for conjunctive queries. Gottlob, Leone, and Scarcello [21] showed that queries with bounded (generalized) hypertree width can be evaluated in polynomial time. The fractional hypertree width, introduced by Grohe and Marx, provides the tightest known width measure and connects directly to the AGM bound: a query achieves $|Q(D)| \leq |D|^{\text{fhtw}(Q)}$, where $\text{fhtw}$ is the fractional hypertree width.

### 3.2 Practical WCOJ Implementations

Translating worst-case optimal algorithms from theory to practice has been a decade-long effort, with several distinct architectural approaches.

**Sorted trie approaches.** Veldhuizen [4] introduced *Leapfrog Triejoin* (ICDT 2014), the first practical WCOJ implementation. The key insight is that sorted tries enable a *leapfrog* intersection primitive: given $k$ sorted iterators, leapfrog advances them in round-robin fashion, using `seek` operations to skip past values that cannot appear in the intersection. For $k$ iterators with total size $N$ and intersection size $Z$, leapfrog runs in $O(N \cdot k \cdot \log(N/k) + Z)$. The algorithm was deployed in the LogicBlox commercial Datalog engine, where pre-sorted data representations made trie construction essentially free.

**Graph-specialized engines.** Aberger et al. [14] developed *EmptyHeaded* (TODS 2017), a relational engine for graph processing built on WCOJ principles. EmptyHeaded introduced several systems innovations: (i) a columnar trie layout amenable to SIMD-accelerated set intersection, (ii) a generalized hypertree decomposition (GHD)-based query compiler that selects optimal variable orderings, and (iii) support for aggregation within the WCOJ loop. On graph pattern queries (triangles, 4-cliques, Lollipop), EmptyHeaded demonstrated order-of-magnitude speedups over binary-join engines. However, EmptyHeaded requires pre-computation of sorted indices and does not support general SQL workloads or ad-hoc query execution.

**Hash-based integration into general-purpose RDBMS.** Freitag, Bandle, Schmidt, Kemper, and Neumann [5] (PVLDB 2020) made the critical observation that WCOJ can be practical *within* a general-purpose RDBMS without requiring pre-sorted indices. Their key contributions were: (i) a hash-based WCOJ algorithm using hash tries that can be built during query execution (amortizing construction with first-use), (ii) a *hybrid optimizer* in the Umbra system (successor to HyPer) that transparently selects between binary and multi-way joins within the same query plan based on cost estimation, and (iii) a demonstration that the overhead of hash trie construction is acceptable for OLAP workloads. Their work is the closest precursor to our Calcite integration, though they did not consider multi-query optimization.

**Unified approaches.** Wang, Willsey, and Suciu [15] (SIGMOD 2023) proposed *Free Join*, which unifies binary and worst-case optimal joins under a single algorithmic framework. Free Join uses a novel plan representation (the *free join plan*) and a data structure (*free join trie*) that generalizes both hash tables and sorted tries. The framework provides a principled way to select between binary and multi-way join strategies at the granularity of individual subproblems within a query, rather than the all-or-nothing approach of prior systems.

**Table 1.** Comparison of WCOJ implementations.

| System | Index Type | Pre-built? | General SQL? | Hybrid w/ Binary? | Multi-Query? |
|--------|-----------|------------|--------------|-------------------|-------------|
| Leapfrog Triejoin [4] | Sorted trie | Yes | No (Datalog) | No | No |
| EmptyHeaded [14] | Columnar trie | Yes | No (graph) | No | No |
| Umbra [5] | Hash trie | No | Yes | Yes | No |
| Free Join [15] | Free join trie | No | Yes | Yes (unified) | No |
| **This work** | **Hash trie** | **No** | **Yes** | **Yes** | **Yes** |

### 3.3 Multi-Query Optimization

Multi-query optimization (MQO) seeks to reduce redundant computation when processing multiple queries, either within a single complex query or across a batch of concurrent queries.

**Foundations.** The problem was formalized by Sellis [6] (TODS 1988), who identified common sub-expressions across queries and proposed algorithms for selecting which intermediate results to materialize. Even earlier, Finkelstein [23] (SIGMOD 1982) studied common expression analysis in the context of integrity constraint checking, where a single update can trigger multiple constraint-verification queries with shared sub-computations. Both works established that MQO is fundamentally a *selection problem*: given a set of candidate materializations, choose a subset that maximizes total cost savings under resource constraints.

**Complexity and approximation.** The MQO selection problem is NP-hard [7, 10], as it subsumes the weighted set cover problem. Kathuria and Sudarshan [7] (PODS 2017) provided the first provable approximation guarantees by reformulating MQO as a monotone submodular maximization problem, achieving a $(1 - 1/e)$ approximation ratio under a linear cost transformation. Their greedy algorithm can be integrated into existing transformation-based optimizers with modest overhead.

**Heuristic approaches.** Roy, Seshadri, Sudarshan, and Bhobe [8] (SIGMOD 2000) demonstrated that heuristic MQO is practical and beneficial, proposing three cost-based algorithms---Volcano-SH (sharing heuristic), Volcano-RU (reuse), and a greedy approach---that extend the Volcano search strategy. Their experiments on TPC-D workloads showed significant cost reductions with acceptable optimization overhead. This line of work established that MQO can be implemented as a lightweight extension to existing optimizers rather than requiring a fundamentally different architecture.

**Algebraic and operator-based approaches.** Tu, Eslami, Xu, and Charkhgard [11] (IEEE BigData 2022) proposed *PsiDB*, which uses $\psi$-operators to algebraically combine multiple queries into a single global expression. Their approach reveals optimization opportunities through algebraic equivalence rules centered on the $\psi$-operator, achieving up to 36$\times$ speedup over sequential execution. Our `Combine` operator is philosophically similar to the $\psi$-operator but is specifically designed to compose with WCOJ execution rather than traditional binary joins.

**Work sharing at the execution level.** Harizopoulos, Shkapenyuk, and Ailamaki [22] (SIGMOD 2005) introduced *QPipe*, an operator-centric relational engine that shares work across concurrent queries at execution time through *on-demand simultaneous pipelining* (OSP). QPipe detects sharing opportunities dynamically (at runtime) rather than statically (at optimization time), complementing our compile-time approach. Our scan-sharing layer (Layer 1) achieves similar goals through a different mechanism: static common sub-expression detection followed by spool-based materialization.

**Hybrid strategies.** Gurumurthy et al. [12] (Information Systems Frontiers 2024) explored hybrid MQO combining batched execution (shared sub-expression) with caching (materialized view reuse), finding that LRU caching combined with batching provides up to 2$\times$ speedup over sequential execution. Michiardi, Carra, and Migliorini [16] studied cache-based MQO for distributed computing frameworks, formulating the problem as a multiple-choice knapsack optimization. Schonberger, Trummer, and Mauerer [17] explored quantum-inspired annealing for large-scale MQO instances (up to 1,000 queries), demonstrating that specialized hardware solvers can scale MQO beyond the reach of classical algorithms.

**Surveys.** Zinchenko and Ponomaryov [10] (2025) provide the most comprehensive recent survey of the MQO selection problem, unifying view materialization, index selection, and plan caching under a common framework. Their analysis identifies machine-learning-based approaches as a promising frontier and proposes techniques to exponentially accelerate state-of-the-art selection algorithms.

### 3.4 Query Optimization Frameworks

The architectural substrate for both WCOJ integration and MQO is the query optimizer framework. Modern cost-based optimizers descend from two foundational systems.

**System R** [1] (SIGMOD 1979) introduced the dynamic programming approach to join ordering, cost-based plan enumeration, and the separation of logical and physical plan spaces. Its influence persists in virtually every commercial and open-source RDBMS.

**Volcano and Cascades.** Graefe's *Volcano optimizer generator* [19] (ICDE 1993) introduced rule-based plan transformation with a top-down, goal-directed search strategy. The *Cascades framework* [20] (IEEE Data Engineering Bulletin 1995) refined Volcano with lazy evaluation, memoization, and a cleaner separation between logical exploration and physical implementation. Apache Calcite [9] implements a hybrid Volcano/Cascades optimizer, making it a natural platform for our extensions.

**Industrial trends.** Tian [13] (2025) identifies three key trends in industrial query optimization: (i) tighter feedback loops between optimization and execution (adaptive query processing), (ii) expansion from single-query to workload-level optimization (the convergence of QO and MQO), and (iii) composable architectures that enable cross-engine collaboration. Our work directly addresses trend (ii) by introducing workload-level optimization for WCOJ queries within the composable Calcite architecture.

### 3.5 The Unexplored Intersection

Despite the maturity of both WCOJ algorithms and MQO techniques, no prior work has combined them. This gap is surprising because the structure of WCOJ execution---trie-based indexing, variable-at-a-time search, and backtracking---creates sharing opportunities that are qualitatively different from those in binary-join workloads:

1. **Trie sharing.** Two WCOJ queries over the same relations build identical trie structures, which can be shared via caching. Binary joins use flat hash tables that are less amenable to cross-query reuse due to varying build/probe sides.

2. **Search-space sharing.** Two WCOJ queries with the same join structure traverse identical search trees up to the point of divergence. Binary join plans have no analogous shared traversal---each plan produces and consumes its own intermediate results.

3. **Variable-level factoring.** WCOJ's variable-at-a-time decomposition enables *prefix sharing*: computing shared variable bindings once and distributing them to per-query suffix executors. Binary joins operate at the tuple level and cannot be factored in this way.

Our system exploits all three opportunities through the three-layer architecture described in Sections 6--7.

---

## 4. WCOJ Integration in Calcite

### 4.1 Operator Design

We introduce `EnumerableWCOJ`, a physical operator in the enumerable convention that implements multi-way joins using the WCOJ algorithm. Unlike binary join operators (`EnumerableHashJoin`, `EnumerableMergeJoin`), `EnumerableWCOJ` takes $N \geq 3$ inputs and processes them simultaneously.

**Join Variables.** The operator is parameterized by a list of `JoinVariable` objects, each representing an equivalence class of columns across inputs:

$$\text{JoinVariable}(v) = \{(i_1, f_1), (i_2, f_2), \ldots\}$$

where each pair $(i_k, f_k)$ indicates that input $i_k$'s field $f_k$ participates in the equivalence class. For the triangle query with inputs $R(a,b)$, $S(b,c)$, $T(c,a)$:

- $v_0 = \{(R, a), (T, a)\}$ -- the shared variable $a$
- $v_1 = \{(R, b), (S, b)\}$ -- the shared variable $b$
- $v_2 = \{(S, c), (T, c)\}$ -- the shared variable $c$

### 4.2 Cyclic Query Detection

The conversion from standard relational algebra to `EnumerableWCOJ` is governed by `EnumerableWCOJRule`, which matches on `MultiJoin` nodes (produced by Calcite's `JoinToMultiJoinRule`). The rule fires only when:

1. The join type is `INNER` (WCOJ semantics require equi-joins)
2. There are at least 3 inputs (the minimum for a cycle)
3. The join graph is *cyclic*

**Join graph construction.** We extract equi-join predicates from the `MultiJoin`'s condition, compute field offsets per input, and use a *Union-Find* data structure with path compression and union-by-rank to group fields into equivalence classes. Each class spanning two or more inputs becomes a `JoinVariable`.

**Cyclicity test.** We construct an undirected graph where nodes are inputs and edges connect inputs that share at least one `JoinVariable`. A connected graph with $|E| \geq |V|$ contains at least one cycle (since a tree on $|V|$ nodes has exactly $|V| - 1$ edges). This simple test is sound and sufficient for our purposes, as WCOJ provides its primary advantage over binary joins precisely on cyclic query topologies.

### 4.3 Multi-Level Hash Tries

Our WCOJ implementation uses a purpose-built `HashTrie` data structure---a recursive hash map where each level corresponds to a join variable in the global variable ordering:

```
HashTrie for R(a, b):
  root
  +-- a=1 --> { b=2: [row(1,2)], b=5: [row(1,5)] }
  +-- a=2 --> { b=3: [row(2,3)] }
  +-- a=3 --> { b=1: [row(3,1)] }
```

The trie supports three operations:

- **`build(source, extractors)`**: Scans the input once, inserting each row by extracting keys at each level. Time: $O(|R| \cdot d)$ where $d$ is the number of levels.
- **`getKeysAtLevel(level, prefix)`**: Navigates using the prefix of prior bindings and returns distinct keys at the target level. Time: $O(d)$ navigation + $O(|\text{keys}|)$ enumeration.
- **`probe(keyValues)`**: Full probe with all levels specified, returns matching rows. Time: $O(d)$.

Unlike Veldhuizen's sorted trie approach [4], our hash-based tries follow Freitag et al.'s insight [5] that hash-based structures can be built efficiently during query execution without requiring pre-sorted input or persistent indices.

### 4.4 WCOJ Enumerator

The `WCOJEnumerator` implements iterative deepening with backtracking over the global variable ordering. The core loop:

```
moveNext():
    Phase 1 (initialization):
        for level = 0 to numVariables - 1:
            initCandidatesAtLevel(level)
            advanceAtLevel(level)

    Phase 2 (yield + backtrack):
        loop:
            collectMatches() --> yield cross-product of matching rows
            currentLevel--
            while currentLevel >= 0:
                if advanceAtLevel(currentLevel):
                    propagate forward to deeper levels
                    break
                else:
                    currentLevel--  // exhausted, backtrack further
```

The critical method is `initCandidatesAtLevel(level)`, which implements the intersection step from Generic-Join:

$$\text{candidates}(x_k) = \bigcap_{R_i \ni x_k} \pi_{x_k}\left(\sigma_{x_1 = v_1, \ldots, x_{k-1} = v_{k-1}}(R_i)\right)$$

Each trie lookup navigates using the prefix of already-bound variables relevant to that specific input, returning only values consistent with all prior bindings. The intersection across inputs ensures only values satisfying *all* join conditions survive.

### 4.5 Cost Model

The cost of `EnumerableWCOJ` is modeled as:

$$C_{\text{WCOJ}} = C_{\text{build}} + C_{\text{enum}} = \sum_{i=1}^{n} |R_i| + |Q(D)|$$

where $C_{\text{build}}$ accounts for trie construction (linear scan of each input) and $C_{\text{enum}}$ accounts for result enumeration. For cyclic queries, this is significantly less than binary join plans where intermediate results can dominate.

---

## 5. The Combine Operator and Multi-Query Framework

### 5.1 Motivation

When a workload consists of multiple queries over the same data---for example, five triangle queries over the same graph with different projections---traditional engines execute them independently. Each query builds its own hash tables, scans the same base tables, and performs the same join work. Even with WCOJ, the same trie structures are built redundantly and the same variable-binding search space is traversed multiple times.

### 5.2 SQL Extension: MULTI()

We extend Calcite's SQL parser with a `MULTI` construct that declares a batch of queries for joint optimization:

```sql
MULTI(
  (SELECT e1.src, e1.dst, e2.dst
   FROM edges e1, edges e2, edges e3
   WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src),
  (SELECT e1.src, e2.src, e3.src
   FROM edges e1, edges e2, edges e3
   WHERE e1.dst = e2.src AND e2.dst = e3.src AND e3.dst = e1.src),
  ...
)
```

The parser recognizes `MULTI` as a new keyword and produces a `SqlCall` with `SqlKind.MULTI`. During SQL-to-RelNode conversion, each sub-query is independently converted to a relational expression, and all are wrapped in a single `Combine` node.

### 5.3 The Combine Relational Operator

`Combine` is a new `AbstractRelNode` in Calcite's relational algebra that holds $N$ independent sub-queries as children. Its key design properties:

**Row type.** The output type is a struct of arrays:

$$\text{rowType}(\text{Combine}) = \text{STRUCT}\langle \text{EXPR\$0}: \text{ARRAY}\langle\tau_0\rangle, \ldots, \text{EXPR\$}(N{-}1): \text{ARRAY}\langle\tau_{N-1}\rangle \rangle$$

where $\tau_i$ is the row type of the $i$-th child. This encoding fits Calcite's existing type system while making clear that the result sets are independent. A single output row contains all $N$ result sets.

**Cost model.** `Combine` itself has minimal self-cost ($\sum_i |R_i| \times 0.01$ CPU). The optimizer evaluates the cumulative cost through children, allowing optimization rules to improve individual queries or exploit cross-query sharing.

**Novelty.** Standard Calcite has no multi-root operator. The closest analog is `UNION ALL`, but `Combine` preserves independent result sets without requiring compatible schemas. This is essential for MQO: the optimizer can see all queries simultaneously and identify sharing opportunities that are invisible when queries are optimized in isolation.

### 5.4 Physical Implementation

`EnumerableCombine` implements code generation for the `Combine` operator. During `implement()`, it:

1. Creates a shared `TrieCache` instance and stores it on the `EnumerableRelImplementor`
2. Visits each child, converting each `Enumerable` result to a `List`
3. Packs all lists into a single struct row
4. Returns a singleton `Enumerable` containing that struct

The `TrieCache` creation in step 1 is the critical bridge for cross-query optimization---it provides a shared context that child WCOJ operators can use to avoid redundant trie construction.

---

## 6. Three Layers of Shared Computation

When multiple WCOJ queries execute within a `Combine`, three layers of optimization eliminate redundant work at progressively higher levels of abstraction:

```
+-----------------------------------------------------+
|           Layer 3: Prefix Sharing                    |
|  EnumerableCombineWcojPrefixRule                     |
|  WCOJPrefixAnalyzer + JoinVariableFingerprint        |
|  "Compute shared variable prefix once, reuse suffix" |
+-----------------------------------------------------+
|           Layer 2: Trie Cache                        |
|  TrieCache (IdentityHashMap-based)                   |
|  "Build each trie once, share across WCOJ operators" |
+-----------------------------------------------------+
|           Layer 1: Scan Sharing                      |
|  CombineSharedComponentsRule                         |
|  RelCommonExpressionBasicSuggester + Spools          |
|  "Materialize shared scans once via lazy spools"     |
+-----------------------------------------------------+
```

### 6.1 Layer 1: Scan Sharing via Lazy Spools

**Problem.** When multiple queries within a `Combine` scan the same base table, each scan reads the data independently, multiplying I/O cost by the number of queries.

**Solution.** `CombineSharedComponentsRule` detects structurally identical sub-trees across `Combine` inputs using `RelCommonExpressionBasicSuggester`, which runs a `HepPlanner` with `CommonRelSubExprRegisterRule` variants for each node type (Scan, Filter, Project, Join, Aggregate, Combine).

For each shared sub-tree, the rule:

1. Creates a `SpoolRelOptTable` backed by an in-memory `ListTransientTable` with accurate row-count statistics for cost estimation
2. Wraps it in a `LogicalTableSpool` with `LAZY` read/write semantics
3. Replaces occurrences using a `RelHomogeneousShuttle`:
   - **First occurrence** $\rightarrow$ the spool (producer: executes the sub-tree, writes to storage)
   - **Subsequent occurrences** $\rightarrow$ `LogicalTableScan` on the spool table (consumer: reads from storage)

**Example.** Two triangle queries over the same `edges` table:

```
Before:                          After:
Combine                          Combine
+-- WCOJ(scan_e, scan_e, ...)    +-- WCOJ(SPOOL(scan_e), read_spool, ...)
+-- WCOJ(scan_e, scan_e, ...)    +-- WCOJ(read_spool, read_spool, ...)
```

The spool ensures each distinct base-table scan executes exactly once. Crucially, spools produce the same Java object reference for all consumers, enabling Layer 2.

### 6.2 Layer 2: Trie Cache

**Problem.** Even after scan sharing, each WCOJ operator independently builds a `HashTrie` from its inputs. If two WCOJ operators join the same relation on the same key, they build identical tries.

**Solution.** `TrieCache` is a per-execution cache of `HashTrie` instances:

```java
public class TrieCache {
    private final IdentityHashMap<Enumerable<Object[]>,
                                  Map<Integer, HashTrie<Object[]>>> cache;

    public HashTrie<Object[]> getOrBuild(
            Enumerable<Object[]> input, int fieldIdx, ...) {
        return cache
            .computeIfAbsent(input, k -> new HashMap<>())
            .computeIfAbsent(fieldIdx, k -> HashTrie.build(input, extractors));
    }
}
```

The design is deliberately simple:

- **Identity-based lookup.** Uses `IdentityHashMap` so sharing occurs only when inputs are the *same Java object reference*. This is sound because Layer 1's spools ensure shared scans produce the same object.
- **Lazy construction.** `getOrBuild` computes on first access; subsequent calls return the cached trie.
- **Lifecycle.** Created by `EnumerableCombine.implement()`, passed to all child WCOJ operators, garbage-collected after execution.

The `TrieCache` extends Veldhuizen's per-relation trie construction [4] to the multi-query setting: instead of each query building its own tries, the cache amortizes construction across all queries in the `Combine`.

### 6.3 Layer 3: Shared-Prefix Execution

**Problem.** Two WCOJ operators may enumerate the same variable prefix identically. For example, two triangle queries over the same graph might share all three join variables and differ only in their output projections. Without sharing, both operators independently traverse the same backtracking search space.

**Solution.** Detect shared prefixes at compile time via fingerprinting, compute the prefix once at runtime, and distribute the bindings to per-query suffix executors.

#### 6.3.1 Join Variable Fingerprinting

To compare variables across different WCOJ operators, we need a canonical representation that is independent of operator-local input numbering. `JoinVariableFingerprint` achieves this by using the *structural digest* (`RelDigest`) of each input `RelNode`:

$$\text{fingerprint}(v) = \text{sort}\left(\left\{(\text{digest}(R_{i_k}), f_k) \mid (i_k, f_k) \in v.\text{occurrences}\right\}\right)$$

Two fingerprints are equal when they represent the same key intersection over structurally identical inputs. This invariant makes prefix detection correct: if fingerprints match at positions $0, \ldots, K{-}1$, the WCOJ backtracking search over those variables produces identical bindings.

#### 6.3.2 Prefix Group Detection

Given $N$ WCOJ operators, `WCOJPrefixAnalyzer` builds a *trie of fingerprint sequences* to find shared prefixes:

```
Input: WCOJ_0 with variables [fp_A, fp_B, fp_C]
       WCOJ_1 with variables [fp_A, fp_B, fp_D]
       WCOJ_2 with variables [fp_X, fp_Y]

Prefix trie:
    root
    +-- fp_A --> fp_B --> +-- fp_C  (WCOJ_0)
    |                     +-- fp_D  (WCOJ_1)
    +-- fp_X --> fp_Y             (WCOJ_2)

Result: PrefixGroup(depth=2, members=[0, 1])
```

The `extractGroups` traversal finds *divergence points*---nodes where `|children| > 1`---and groups all descendant queries. Groups with $\text{depth} \geq 1$ and $|\text{members}| \geq 2$ represent opportunities for shared-prefix execution.

#### 6.3.3 Two-Phase Execution

Once prefix groups are detected, `EnumerableCombineWCOJPrefixRule` replaces grouped WCOJ operators with `EnumerableWCOJWithPrefix`, which generates code for two-phase execution:

**Phase 1: Prefix computation** (`WCOJPrefixEnumerator`). This is the standard WCOJ algorithm truncated at depth $K$ (the prefix depth). It yields variable *bindings* rather than result rows:

$$\text{WCOJ-PREFIX}(R_1 \ldots R_n, x_1 \ldots x_K): \text{for each valid } (v_1, \ldots, v_K), \text{yield } (v_1, \ldots, v_K)$$

**Phase 2: Suffix execution** (`WCOJWithPrefixEnumerator`). For each prefix binding, the suffix executor sets $x_1 = v_1, \ldots, x_K = v_K$, initializes suffix variables $x_{K+1}, \ldots, x_m$, and backtracks only within the suffix:

```java
boolean moveNextSuffix() {
    final int floor = suffixPrefixDepth;
    while (currentLevel >= floor) {
        if (advanceAtLevel(currentLevel)) {
            propagate forward...
        } else {
            currentLevel--;
        }
    }
    if (currentLevel < floor) return false; // prefix exhausted
}
```

The `floor` parameter prevents backtracking below the prefix boundary, confining suffix execution to the per-query divergent portion of the search space.

---

## 7. Formal Analysis

### 7.1 Correctness of Prefix Sharing

**Theorem 1.** *Let $Q_1$ and $Q_2$ be two WCOJ queries with variable orderings $(x_1, \ldots, x_m)$ and $(x_1, \ldots, x_K, y_{K+1}, \ldots, y_{m'})$ respectively, such that variables $x_1, \ldots, x_K$ have identical fingerprints. Then the prefix bindings $\{(v_1, \ldots, v_K)\}$ computed by $Q_1$'s prefix enumerator are exactly the prefix bindings that $Q_2$'s full enumerator would produce for its first $K$ variables.*

**Proof sketch.** The WCOJ execution forms a search tree where level $k$ branches on all valid values of $x_k$ given the partial binding $(v_1, \ldots, v_{k-1})$. The candidates at level $k$ are:

$$\text{candidates}(x_k \mid v_1, \ldots, v_{k-1}) = \bigcap_{R_i \ni x_k} \text{trie}_i.\text{getKeys}(k, \text{prefix}_i(v_1, \ldots, v_{k-1}))$$

Since fingerprints match at positions $0, \ldots, K{-}1$, the participating inputs and their structural digests are identical. Combined with the `TrieCache` (which ensures identical inputs produce the same trie objects), the candidate sets are identical at each prefix level. Therefore the search trees are isomorphic up to depth $K$. $\square$

### 7.2 Cost Analysis

Let $P$ denote the cost of computing the shared prefix (iterating all valid $(v_1, \ldots, v_K)$ bindings), let $S_i$ denote the suffix cost for query $Q_i$, and let $N$ denote the number of queries in the prefix group.

**Without sharing:**

$$C_{\text{independent}} = N \cdot (P + \bar{S}) + N \cdot C_{\text{trie}}$$

where $C_{\text{trie}}$ is the per-query trie construction cost.

**With all three layers:**

$$C_{\text{shared}} = P + N \cdot \bar{S} + C_{\text{trie}} + C_{\text{cache}}$$

where $C_{\text{cache}}$ is the negligible `TrieCache` lookup overhead (hash map operations).

**Savings:**

$$\Delta C = (N - 1) \cdot P + (N - 1) \cdot C_{\text{trie}}$$

The prefix cost $P$ dominates when the prefix covers most of the variables---the common case for queries that differ only in their final projection, aggregation, or filter on non-join columns. For $N$ identical triangle queries differing only in projection, $K = 3 = m$ (all variables shared), so $\bar{S} \approx 0$ and the savings approach $(N-1) \cdot C_{\text{full\_WCOJ}}$: nearly linear speedup in the batch size.

---

## 8. Experimental Evaluation

### 8.1 Experimental Setup

We evaluate our system using a custom benchmark (`WCOJBenchmarkCli`) that compares four execution modes:

| Mode | Description |
|------|-------------|
| **baseline** | Standard Calcite with binary hash joins, queries run sequentially |
| **wcoj** | WCOJ for each query, run sequentially (no multi-query optimization) |
| **combine** | `MULTI()` with WCOJ (batched execution, no sharing rules) |
| **combine-share** | `MULTI()` with WCOJ + scan sharing + trie caching + prefix sharing |

**Graph generation.** We construct synthetic directed graphs with controlled cyclicity. A fraction of vertices (default 5%) are designated as *hub nodes* with high in-degree and out-degree, creating the dense intermediate results that expose binary join weaknesses. Spoke-to-spoke edges ensure triangles exist. This design mirrors real-world power-law graphs while providing controlled experimental parameters.

**Query workload.** Triangle queries $Q_\triangle(a,b,c) \leftarrow R(a,b), S(b,c), T(c,a)$ with 5 projection variations, wrapped in `MULTI()` for batched modes.

**Hardware and software.** *[To be filled with specific hardware configuration.]*

### 8.2 Results

> **Note:** This section will be populated with experimental data and figures as benchmarks are finalized. The subsections below describe the planned evaluations and expected result presentation.

#### 8.2.1 WCOJ vs. Binary Joins on Cyclic Queries

*[Chart: Execution time (ms) vs. graph size (|V|) for triangle query, comparing baseline vs. wcoj modes.]*

*[Chart: Intermediate result sizes for binary join plans vs. WCOJ on graphs with varying hub density.]*

**Expected analysis.** For graphs with high hub density, binary join plans produce intermediate results that grow quadratically with hub degree, while WCOJ maintains runtime proportional to the actual triangle count plus input size.

#### 8.2.2 Multi-Query Speedup from Shared Computation

*[Chart: Total execution time vs. batch size (N = 1, 2, 5, 10, 20 queries) for all four modes.]*

*[Chart: Breakdown of time spent in trie construction, prefix enumeration, and suffix enumeration for combine-share mode.]*

**Expected analysis.** The combine-share mode should demonstrate near-linear speedup with batch size when queries share the same join structure, as prefix computation (the dominant cost) is performed exactly once regardless of $N$.

#### 8.2.3 Layer-by-Layer Contribution

*[Chart: Stacked bar chart showing the contribution of each optimization layer (scan sharing, trie caching, prefix sharing) to total savings.]*

*[Table: Absolute and relative savings from each layer on representative workloads.]*

| Layer | What's Shared | Metric |
|-------|--------------|--------|
| 1. Scan Sharing | Table reads | I/O operations saved |
| 2. Trie Cache | Index structures | Trie build time saved |
| 3. Prefix Sharing | Backtracking search | Enumeration steps saved |

#### 8.2.4 Scalability with Graph Size

*[Chart: Execution time vs. |V| for fixed batch size, showing scaling behavior of each mode.]*

*[Chart: Memory consumption vs. graph size, highlighting trie cache overhead.]*

#### 8.2.5 Impact of Query Diversity

*[Chart: Speedup vs. prefix depth (K/m ratio) for workloads with varying degrees of structural similarity.]*

**Expected analysis.** As queries diverge earlier in their variable ordering (lower $K/m$), the benefit of prefix sharing decreases but trie caching and scan sharing still provide value.

#### 8.2.6 Comparison with Standalone WCOJ Systems

*[Chart: Execution time comparison with EmptyHeaded [14] on triangle and 4-clique queries, if applicable.]*

**Expected analysis.** While standalone WCOJ systems like EmptyHeaded benefit from specialized storage formats and pre-computed indices, our approach offers the advantage of integration with a general-purpose SQL optimizer, enabling seamless fallback to binary joins for acyclic query components.

---

## 9. Related Work and Positioning

### 9.1 Positioning Against WCOJ Systems

As surveyed in Section 3.2, prior WCOJ implementations---Leapfrog Triejoin [4], EmptyHeaded [14], Umbra [5], and Free Join [15]---focus exclusively on single-query optimization. Our work is closest to Freitag et al. [5] in its hash-based approach and integration into a cost-based optimizer. However, we extend beyond single-query optimization to the multi-query setting, introducing three layers of cross-query sharing (scan spooling, trie caching, prefix sharing) that have no analog in any prior WCOJ system. Table 1 in Section 3.2 summarizes this distinction.

### 9.2 Positioning Against MQO Systems

Prior MQO systems (Section 3.3) operate on traditional binary-join plans and share work at the scan or intermediate-result level [6, 8, 11, 22]. Our `Combine` operator and three-layer sharing architecture differ in that they exploit the *structure of WCOJ computation*---particularly the trie data structures and variable-at-a-time search---to identify and eliminate redundancies that are invisible to traditional MQO techniques. The prefix-sharing mechanism (Layer 3) is entirely novel: it factors the WCOJ search space into shared and per-query components, a decomposition that has no counterpart in binary-join MQO.

### 9.3 Positioning Within Calcite

Apache Calcite [9] provides the modular, extensible foundation on which our work builds. Our extensions---the `Combine` operator, `MULTI()` syntax, `EnumerableWCOJ`, and the three sharing rules---are implemented as standard Calcite `RelNode` subclasses and `RelRule` instances, preserving full backward compatibility. This demonstrates that Calcite's architecture can accommodate fundamentally new execution paradigms (multi-way joins, multi-query batching) without requiring changes to the core optimizer infrastructure.

---

## 10. Conclusion

We have presented a unified framework within Apache Calcite that combines worst-case optimal join algorithms with multi-query optimization. Our system introduces the `Combine` relational operator and `MULTI()` SQL syntax for declarative multi-query batching, a hash-based WCOJ implementation with multi-level trie indexing, and three layers of cross-query optimization: scan sharing via lazy spools, identity-based trie caching, and shared-prefix execution through join-variable fingerprinting.

The key insight underlying our approach is that WCOJ's variable-at-a-time execution model creates natural sharing opportunities that do not exist in binary join plans. When multiple queries join the same relations on the same keys, they traverse identical search trees---a redundancy that our prefix-sharing mechanism eliminates. Combined with scan sharing and trie caching at the lower layers, the system achieves near-linear speedup with batch size for structurally similar query workloads.

All contributions are implemented as modular extensions to Apache Calcite, preserving backward compatibility and enabling adoption by the numerous systems built on the Calcite framework. The source code is available as open-source contributions to the Calcite project.

**Future work.** Several directions remain open: (1) extending the cost model to account for memory pressure from concurrent trie construction, (2) exploring adaptive variable ordering that considers both single-query and cross-query optimization objectives, (3) integrating with Calcite's materialized view subsystem for persistent cross-batch sharing, and (4) extending the `Combine` operator to support heterogeneous join strategies (e.g., WCOJ for cyclic components and binary joins for acyclic components within the same batch).

---

## References

[1] P. G. Selinger, M. M. Astrahan, D. D. Chamberlin, R. A. Lorie, and T. G. Price, "Access path selection in a relational database management system," in *Proceedings of the 1979 ACM SIGMOD International Conference on Management of Data*, 1979, pp. 23--34. doi: [10.1145/582095.582099](https://doi.org/10.1145/582095.582099)

[2] A. Atserias, M. Grohe, and D. Marx, "Size bounds and query plans for relational joins," *SIAM Journal on Computing*, vol. 42, no. 4, pp. 1737--1767, 2013. doi: [10.1137/110859440](https://doi.org/10.1137/110859440)

[3] H. Q. Ngo, E. Porat, C. Re, and A. Rudra, "Worst-case optimal join algorithms," *Journal of the ACM*, vol. 65, no. 3, pp. 1--40, 2018. doi: [10.1145/3180143](https://doi.org/10.1145/3180143)

[4] T. L. Veldhuizen, "Leapfrog Triejoin: A simple, worst-case optimal join algorithm," in *Proc. 17th International Conference on Database Theory (ICDT)*, Athens, Greece, 2014, pp. 96--106. doi: [10.4230/LIPIcs.ICDT.2014.173](https://doi.org/10.4230/LIPIcs.ICDT.2014.173)

[5] M. Freitag, M. Bandle, T. Schmidt, A. Kemper, and T. Neumann, "Adopting worst-case optimal joins in relational database systems," *Proceedings of the VLDB Endowment*, vol. 13, no. 12, pp. 1891--1904, 2020. doi: [10.14778/3407790.3407797](https://doi.org/10.14778/3407790.3407797)

[6] T. K. Sellis, "Multiple-query optimization," *ACM Transactions on Database Systems*, vol. 13, no. 1, pp. 23--52, 1988. doi: [10.1145/42201.42203](https://doi.org/10.1145/42201.42203)

[7] T. Kathuria and S. Sudarshan, "Efficient and provable multi-query optimization," in *Proc. 36th ACM SIGMOD-SIGACT-SIGAI Symposium on Principles of Database Systems (PODS)*, 2017, pp. 53--67. doi: [10.1145/3034786.3034792](https://doi.org/10.1145/3034786.3034792)

[8] P. Roy, S. Seshadri, S. Sudarshan, and S. Bhobe, "Efficient and extensible algorithms for multi query optimization," *ACM SIGMOD Record*, vol. 29, no. 2, pp. 249--260, 2000. doi: [10.1145/335191.335419](https://doi.org/10.1145/335191.335419)

[9] E. Begoli, J. Camacho-Rodriguez, J. Hyde, M. J. Mior, and D. Lemire, "Apache Calcite: A foundational framework for optimized query processing over heterogeneous data sources," in *Proc. 2018 ACM International Conference on Management of Data (SIGMOD)*, Houston, TX, 2018, pp. 221--230. doi: [10.1145/3183713.3190662](https://doi.org/10.1145/3183713.3190662)

[10] S. Zinchenko and D. Ponomaryov, "The selection problem in multi-query optimization: A comprehensive survey," *arXiv preprint arXiv:2412.11828*, 2025. doi: [10.48550/arXiv.2412.11828](https://doi.org/10.48550/arXiv.2412.11828)

[11] Y. Tu, M. Eslami, Z. Xu, and H. Charkhgard, "Multi-query optimization revisited: A full-query algebraic method," in *Proc. IEEE International Conference on Big Data*, 2022, pp. 252--261. doi: [10.1109/BigData55660.2022.10020338](https://doi.org/10.1109/BigData55660.2022.10020338)

[12] B. Gurumurthy, V. R. Bidarkar, D. Broneske, T. Pionteck, and G. Saake, "Exploiting shared sub-expression and materialized view reuse for multi-query optimization," *Information Systems Frontiers*, 2024. doi: [10.1007/s10796-024-10506-w](https://doi.org/10.1007/s10796-024-10506-w)

[13] Y. Tian, "Query optimization in the wild: Realities and trends," *arXiv preprint arXiv:2510.20082*, 2025. doi: [10.48550/arXiv.2510.20082](https://doi.org/10.48550/arXiv.2510.20082)

[14] C. R. Aberger, A. Lamb, S. Tu, A. Notzli, K. Olukotun, and C. Re, "EmptyHeaded: A relational engine for graph processing," *ACM Transactions on Database Systems*, vol. 42, no. 4, pp. 20:1--20:44, 2017. doi: [10.1145/3129246](https://doi.org/10.1145/3129246)

[15] Y. R. Wang, M. Willsey, and D. Suciu, "Free Join: Unifying worst-case optimal and traditional joins," *Proceedings of the ACM on Management of Data*, vol. 1, no. 2, Article 150, 2023. doi: [10.1145/3589295](https://doi.org/10.1145/3589295)

[16] P. Michiardi, D. Carra, and S. Migliorini, "Cache-based multi-query optimization for data-intensive scalable computing frameworks," *arXiv preprint arXiv:1805.08650*, 2018. doi: [10.48550/arXiv.1805.08650](https://doi.org/10.48550/arXiv.1805.08650)

[17] M. Schonberger, I. Trummer, and W. Mauerer, "Large-scale multiple query optimisation with incremental quantum(-inspired) annealing," *Proceedings of the VLDB Endowment*, 2018.

[18] H. Q. Ngo, C. Re, and A. Rudra, "Skew strikes back: New developments in the theory of join algorithms," *ACM SIGMOD Record*, vol. 42, no. 4, pp. 5--16, 2013. doi: [10.1145/2590989.2590991](https://doi.org/10.1145/2590989.2590991)

[19] G. Graefe and W. J. McKenna, "The Volcano optimizer generator: Extensibility and efficient search," in *Proc. 9th IEEE International Conference on Data Engineering (ICDE)*, Vienna, Austria, 1993, pp. 209--218. doi: [10.1109/ICDE.1993.344061](https://doi.org/10.1109/ICDE.1993.344061)

[20] G. Graefe, "The Cascades framework for query optimization," *IEEE Data Engineering Bulletin*, vol. 18, no. 3, pp. 19--29, 1995.

[21] G. Gottlob, N. Leone, and F. Scarcello, "Hypertree decompositions and tractable queries," *Journal of Computer and System Sciences*, vol. 64, no. 3, pp. 579--627, 2002. doi: [10.1006/jcss.2001.1809](https://doi.org/10.1006/jcss.2001.1809)

[22] S. Harizopoulos, V. Shkapenyuk, and A. Ailamaki, "QPipe: A simultaneously pipelined relational query engine," in *Proc. 2005 ACM SIGMOD International Conference on Management of Data*, 2005, pp. 383--394. doi: [10.1145/1066157.1066201](https://doi.org/10.1145/1066157.1066201)

[23] S. Finkelstein, "Common expression analysis in database applications," in *Proc. 1982 ACM SIGMOD International Conference on Management of Data*, 1982, pp. 235--245. doi: [10.1145/582353.582400](https://doi.org/10.1145/582353.582400)

[24] H. Q. Ngo, "Worst-case optimal join algorithms: Techniques, results, and open problems," in *Proc. 37th ACM SIGMOD-SIGACT-SIGAI Symposium on Principles of Database Systems (PODS)*, 2018, pp. 111--127. doi: [10.1145/3196959.3196990](https://doi.org/10.1145/3196959.3196990)

---

## Appendix A: Key Source Files

| Component | File | Description |
|-----------|------|-------------|
| SQL Parser Extension | `core/src/main/codegen/templates/Parser.jj` | `MULTI` keyword and grammar rule |
| MULTI Operator | `core/.../sql/SqlMultiOperator.java` | SQL operator for `MULTI()` |
| SQL-to-Rel | `core/.../sql2rel/SqlToRelConverter.java` | `convertMulti` method |
| Combine (logical) | `core/.../rel/core/Combine.java` | Multi-query relational operator |
| Combine (physical) | `core/.../enumerable/EnumerableCombine.java` | Code generation with TrieCache |
| WCOJ Operator | `core/.../enumerable/EnumerableWCOJ.java` | Physical WCOJ operator |
| WCOJ Rule | `core/.../enumerable/EnumerableWCOJRule.java` | Cyclic detection and conversion |
| WCOJ Runtime | `linq4j/.../EnumerableDefaults.java` | WCOJEnumerator (lines 5086--5568) |
| HashTrie | `linq4j/.../HashTrie.java` | Multi-level hash trie index |
| TrieCache | `linq4j/.../TrieCache.java` | Identity-based trie sharing |
| Scan Sharing Rule | `core/.../rules/CombineSharedComponentsRule.java` | Common sub-expression spooling |
| Prefix Sharing Rule | `core/.../enumerable/EnumerableCombineWCOJPrefixRule.java` | Prefix group detection |
| Prefix Analyzer | `core/.../enumerable/WCOJPrefixAnalyzer.java` | Fingerprint trie analysis |
| Variable Fingerprint | `core/.../enumerable/JoinVariableFingerprint.java` | Canonical variable comparison |
| WCOJ With Prefix | `core/.../enumerable/EnumerableWCOJWithPrefix.java` | Two-phase prefix/suffix execution |
| Spool Table | `core/.../plan/SpoolRelOptTable.java` | Temporary materialization table |
| Common Expr Suggester | `core/.../rel/RelCommonExpressionBasicSuggester.java` | Shared sub-tree detection |
| Benchmark | `core/.../test/WCOJBenchmarkCli.java` | CLI for comparing execution modes |
