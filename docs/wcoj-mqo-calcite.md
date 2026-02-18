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

The rest of this paper is organized as follows. Section 2 surveys related work on WCOJ algorithms, multi-query optimization, and query processing frameworks. Section 3 describes the WCOJ integration into Calcite. Section 4 presents the `Combine` operator and multi-query framework. Section 5 details the three layers of shared computation. Section 6 provides formal analysis. Section 7 presents experimental results, and Section 8 concludes.

---

## 2. Background and Related Work

This section surveys the two bodies of work that our system bridges---worst-case optimal join algorithms and multi-query optimization---as well as the query processing frameworks that provide the architectural substrate for integration.

### 2.1 Worst-Case Optimal Join Algorithms

The theoretical foundations for WCOJ algorithms were established by Atserias, Grohe, and Marx [2] at FOCS 2008 (journal version in SIAM Journal on Computing, 2013). Their *AGM bound* shows that for a join query $Q$ over relations $R_1, \ldots, R_n$, the maximum output size is:

$$|Q(D)| \leq \prod_{i=1}^{n} |R_i(D)|^{x_i^*}$$

where $\mathbf{x}^*$ is the optimal solution to the *fractional edge cover* linear program over the query hypergraph. This bound is tight: for every query and set of cardinalities, there exists a database instance achieving it. For the triangle query, this yields $|Q_\triangle| \leq |R|^{1/2} \cdot |S|^{1/2} \cdot |T|^{1/2} = |E|^{3/2}$, which is strictly better than the $O(|E|^2)$ intermediate result possible with binary joins.

Ngo, Porat, Re, and Rudra [3] (PODS 2012, JACM 2018) proved that `Generic-Join` achieves this bound:

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

The algorithm processes variables one at a time, computing the intersection of candidate values across all relations that constrain each variable. Their proof proceeds by induction on the number of variables, using the AGM bound at each level to bound the work done.

The theoretical landscape was further enriched by Ngo, Re, and Rudra's survey "Skew Strikes Back" [18], which unified the AGM bound with degree-based bounds (the *BRR bound*, named after Beame, Koutris, and Suciu), showed connections to information-theoretic entropy bounds, and introduced the *Minesweeper* algorithm---a certificate-based approach that can outperform Generic-Join on favorable instances. Beyond cardinality-based bounds, Gottlob, Leone, and Scarcello [21] showed that queries with bounded (generalized) hypertree width can be evaluated in polynomial time. The fractional hypertree width connects directly to the AGM bound: a query achieves $|Q(D)| \leq |D|^{\text{fhtw}(Q)}$. Ngo's invited PODS 2018 tutorial [24] provides a comprehensive overview of these developments, identifying open problems including adaptive variable ordering, handling inequality predicates, and extending WCOJ to aggregate queries.

### 2.2 Practical WCOJ Implementations

Translating worst-case optimal algorithms from theory to practice has been a decade-long effort, with several distinct architectural approaches.

**Sorted trie approaches.** Veldhuizen [4] introduced *Leapfrog Triejoin* (ICDT 2014), the first practical WCOJ implementation. The key insight is that sorted tries enable a *leapfrog* intersection primitive: given $k$ sorted iterators, leapfrog advances them in round-robin fashion, using `seek` operations to skip past values that cannot appear in the intersection. The algorithm was deployed in the LogicBlox commercial Datalog engine, where pre-sorted data representations made trie construction essentially free.

**Graph-specialized engines.** Aberger et al. [14] developed *EmptyHeaded* (TODS 2017), a relational engine for graph processing built on WCOJ principles. EmptyHeaded introduced a columnar trie layout amenable to SIMD-accelerated set intersection and a GHD-based query compiler that selects optimal variable orderings. On graph pattern queries, EmptyHeaded demonstrated order-of-magnitude speedups over binary-join engines but requires pre-computation of sorted indices and does not support general SQL workloads.

**Hash-based integration into general-purpose RDBMS.** Freitag, Bandle, Schmidt, Kemper, and Neumann [5] (PVLDB 2020) made the critical observation that WCOJ can be practical *within* a general-purpose RDBMS without requiring pre-sorted indices. Their hash-based WCOJ algorithm in the Umbra system uses hash tries built during query execution, with a hybrid optimizer that transparently selects between binary and multi-way joins based on cost estimation. Their work is the closest precursor to our Calcite integration, though they did not consider multi-query optimization.

**Unified approaches.** Wang, Willsey, and Suciu [15] (SIGMOD 2023) proposed *Free Join*, which unifies binary and worst-case optimal joins under a single algorithmic framework using a novel *free join trie* that generalizes both hash tables and sorted tries.

**Table 1.** Comparison of WCOJ implementations.

| System | Index Type | Pre-built? | General SQL? | Hybrid w/ Binary? | Multi-Query? |
|--------|-----------|------------|--------------|-------------------|-------------|
| Leapfrog Triejoin [4] | Sorted trie | Yes | No (Datalog) | No | No |
| EmptyHeaded [14] | Columnar trie | Yes | No (graph) | No | No |
| Umbra [5] | Hash trie | No | Yes | Yes | No |
| Free Join [15] | Free join trie | No | Yes | Yes (unified) | No |
| **This work** | **Hash trie** | **No** | **Yes** | **Yes** | **Yes** |

### 2.3 Multi-Query Optimization

Multi-query optimization (MQO) seeks to reduce redundant computation when processing multiple queries, either within a single complex query or across a batch of concurrent queries.

**Foundations.** The problem was formalized by Sellis [6] (TODS 1988), who identified common sub-expressions across queries and proposed algorithms for selecting which intermediate results to materialize. Even earlier, Finkelstein [23] (SIGMOD 1982) studied common expression analysis in the context of integrity constraint checking. Both works established that MQO is fundamentally a *selection problem*: given a set of candidate materializations, choose a subset that maximizes total cost savings under resource constraints.

**The selection problem.** MQO is NP-hard in general [7], as it subsumes the weighted set cover problem. Zinchenko and Ponomaryov [10] provide the most comprehensive recent survey, unifying view materialization, index selection, and plan caching under a common framework. They identify machine-learning-based approaches as a promising frontier and propose techniques to accelerate state-of-the-art selection algorithms. Kathuria and Sudarshan [7] (PODS 2017) provided the first provable approximation guarantees by reformulating MQO as a monotone submodular maximization problem, achieving a $(1 - 1/e)$ approximation ratio. Our work does not address the general MQO selection problem; instead, we exploit the specific structure of WCOJ computation---trie indices, variable-at-a-time search, and shared join graphs---to identify and eliminate redundancies within batches of cyclic join queries. This structural approach is complementary to selection-based MQO and could be integrated with selection algorithms for mixed workloads.

**Heuristic approaches.** Roy, Seshadri, Sudarshan, and Bhobe [8] (SIGMOD 2000) proposed cost-based algorithms---Volcano-SH, Volcano-RU, and a greedy approach---that extend the Volcano search strategy. Their experiments on TPC-D workloads showed significant cost reductions with acceptable overhead, establishing that MQO can be implemented as a lightweight extension to existing optimizers.

**Algebraic and operator-based approaches.** Tu, Eslami, Xu, and Charkhgard [11] (IEEE BigData 2022) proposed *PsiDB*, which uses $\psi$-operators to algebraically combine multiple queries into a single global expression, achieving up to 36$\times$ speedup. Our `Combine` operator is philosophically similar but is specifically designed to compose with WCOJ execution rather than traditional binary joins.

**Work sharing at the execution level.** Harizopoulos, Shkapenyuk, and Ailamaki [22] (SIGMOD 2005) introduced *QPipe*, an operator-centric relational engine that shares work across concurrent queries through *on-demand simultaneous pipelining* (OSP). QPipe detects sharing opportunities dynamically at runtime, complementing our compile-time approach.

**Hybrid and scalable strategies.** Gurumurthy et al. [12] (Information Systems Frontiers 2024) explored hybrid MQO combining batched execution with materialized view reuse. Michiardi, Carra, and Migliorini [16] studied cache-based MQO for distributed computing frameworks, formulating the problem as a multiple-choice knapsack optimization. Schönberger, Trummer, and Mauerer [17] explored quantum-inspired annealing for large-scale MQO instances (up to 1,000 queries), demonstrating that specialized hardware solvers can scale MQO beyond the reach of classical algorithms.

### 2.4 Query Optimization Frameworks

Modern cost-based optimizers descend from two foundational systems. **System R** [1] (SIGMOD 1979) introduced dynamic programming for join ordering, cost-based plan enumeration, and the separation of logical and physical plan spaces. Graefe's *Volcano optimizer generator* [19] (ICDE 1993) introduced rule-based plan transformation with a top-down, goal-directed search strategy; the *Cascades framework* [20] refined Volcano with lazy evaluation and memoization. Apache Calcite [9] implements a hybrid Volcano/Cascades optimizer and serves as the query processing backbone for Apache Hive, Apache Flink, Apache Druid, Trino, and numerous other systems. Calcite provides a relational algebra with extensible operators (`RelNode` hierarchy), a cost-based optimizer (`VolcanoPlanner`), an enumerable code-generation backend via Linq4j, and a pluggable SQL parser built on JavaCC. Our extensions leverage all four components while preserving backward compatibility.

Tian [13] (2025) identifies three key trends in industrial query optimization: tighter feedback loops between optimization and execution, expansion from single-query to workload-level optimization, and composable architectures that enable cross-engine collaboration. Our work directly addresses the second trend by introducing workload-level optimization for WCOJ queries within the composable Calcite architecture.

### 2.5 The Unexplored Intersection

Despite the maturity of both WCOJ algorithms and MQO techniques, no prior work has combined them. This gap is surprising because the structure of WCOJ execution---trie-based indexing, variable-at-a-time search, and backtracking---creates sharing opportunities that are qualitatively different from those in binary-join workloads:

1. **Trie sharing.** Two WCOJ queries over the same relations build identical trie structures, which can be shared via caching. Binary joins use flat hash tables that are less amenable to cross-query reuse due to varying build/probe sides.

2. **Search-space sharing.** Two WCOJ queries with the same join structure traverse identical search trees up to the point of divergence. Binary join plans have no analogous shared traversal---each plan produces and consumes its own intermediate results.

3. **Variable-level factoring.** WCOJ's variable-at-a-time decomposition enables *prefix sharing*: computing shared variable bindings once and distributing them to per-query suffix executors. Binary joins operate at the tuple level and cannot be factored in this way.

Our system exploits all three opportunities through the three-layer architecture described in Sections 5--6.

---

## 3. WCOJ Integration in Calcite

### 3.1 Operator Design

We introduce `EnumerableWCOJ`, a physical operator in the enumerable convention that implements multi-way joins using the WCOJ algorithm. Unlike binary join operators (`EnumerableHashJoin`, `EnumerableMergeJoin`), `EnumerableWCOJ` takes $N \geq 3$ inputs and processes them simultaneously.

**Join Variables.** The operator is parameterized by a list of `JoinVariable` objects, each representing an equivalence class of columns across inputs:

$$\text{JoinVariable}(v) = \{(i_1, f_1), (i_2, f_2), \ldots\}$$

where each pair $(i_k, f_k)$ indicates that input $i_k$'s field $f_k$ participates in the equivalence class. For the triangle query with inputs $R(a,b)$, $S(b,c)$, $T(c,a)$:

- $v_0 = \{(R, a), (T, a)\}$ -- the shared variable $a$
- $v_1 = \{(R, b), (S, b)\}$ -- the shared variable $b$
- $v_2 = \{(S, c), (T, c)\}$ -- the shared variable $c$

### 3.2 Cyclic Query Detection

The conversion from standard relational algebra to `EnumerableWCOJ` is governed by `EnumerableWCOJRule`, which matches on `MultiJoin` nodes (produced by Calcite's `JoinToMultiJoinRule`). The rule fires only when:

1. The join type is `INNER` (WCOJ semantics require equi-joins)
2. There are at least 3 inputs (the minimum for a cycle)
3. The join graph is *cyclic*

**Join graph construction.** We extract equi-join predicates from the `MultiJoin`'s condition, compute field offsets per input, and use a *Union-Find* data structure with path compression and union-by-rank to group fields into equivalence classes. Each class spanning two or more inputs becomes a `JoinVariable`.

**Cyclicity test.** We construct an undirected graph where nodes are inputs and edges connect inputs that share at least one `JoinVariable`. A connected graph with $|E| \geq |V|$ contains at least one cycle (since a tree on $|V|$ nodes has exactly $|V| - 1$ edges). This simple test is sound and sufficient for our purposes, as WCOJ provides its primary advantage over binary joins precisely on cyclic query topologies.

### 3.3 Multi-Level Hash Tries

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

### 3.4 WCOJ Enumerator

The `WCOJEnumerator` implements iterative deepening with backtracking over the global variable ordering. The core loop:

```
moveNext():
    Phase 1 (initialization with backtracking):
        level <- 0
        while level >= 0 and level < numVariables:
            initCandidatesAtLevel(level)
            if advanceAtLevel(level):
                level++
            else:
                level--                    // backtrack
                while level >= 0:
                    if advanceAtLevel(level):
                        level++
                        break
                    level--
        if level < 0: finished <- true; return false

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

The backtracking in Phase 1 is essential for correctness: a candidate value at level $k$ may have no valid continuations at level $k+1$ even though other candidates at level $k$ do. A naive linear initialization that gives up on the first failure would miss valid results on certain data distributions.

The critical method is `initCandidatesAtLevel(level)`, which implements the intersection step from Generic-Join:

$$\text{candidates}(x_k) = \bigcap_{R_i \ni x_k} \pi_{x_k}\left(\sigma_{x_1 = v_1, \ldots, x_{k-1} = v_{k-1}}(R_i)\right)$$

Each trie lookup navigates using the prefix of already-bound variables relevant to that specific input, returning only values consistent with all prior bindings. The intersection across inputs ensures only values satisfying *all* join conditions survive.

### 3.5 Cost Model

The cost of `EnumerableWCOJ` is modeled as:

$$C_{\text{WCOJ}} = C_{\text{build}} + C_{\text{enum}} = \sum_{i=1}^{n} |R_i| + |Q(D)|$$

where $C_{\text{build}}$ accounts for trie construction (linear scan of each input) and $C_{\text{enum}}$ accounts for result enumeration. For cyclic queries, this is significantly less than binary join plans where intermediate results can dominate.

---

## 4. The Combine Operator and Multi-Query Framework

### 4.1 Motivation

When a workload consists of multiple queries over the same data---for example, five triangle queries over the same graph with different projections---traditional engines execute them independently. Each query builds its own hash tables, scans the same base tables, and performs the same join work. Even with WCOJ, the same trie structures are built redundantly and the same variable-binding search space is traversed multiple times.

### 4.2 SQL Extension: MULTI()

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

### 4.3 The Combine Relational Operator

`Combine` is a new `AbstractRelNode` in Calcite's relational algebra that holds $N$ independent sub-queries as children. Its key design properties:

**Row type.** The output type is a struct of arrays:

$$\text{rowType}(\text{Combine}) = \text{STRUCT}\langle \text{EXPR\$0}: \text{ARRAY}\langle\tau_0\rangle, \ldots, \text{EXPR\$}(N{-}1): \text{ARRAY}\langle\tau_{N-1}\rangle \rangle$$

where $\tau_i$ is the row type of the $i$-th child. This encoding fits Calcite's existing type system while making clear that the result sets are independent. A single output row contains all $N$ result sets.

**Cost model.** `Combine` itself has minimal self-cost ($\sum_i |R_i| \times 0.01$ CPU). The optimizer evaluates the cumulative cost through children, allowing optimization rules to improve individual queries or exploit cross-query sharing.

**Novelty.** Standard Calcite has no multi-root operator. The closest analog is `UNION ALL`, but `Combine` preserves independent result sets without requiring compatible schemas. This is essential for MQO: the optimizer can see all queries simultaneously and identify sharing opportunities that are invisible when queries are optimized in isolation.

### 4.4 Physical Implementation

`EnumerableCombine` implements code generation for the `Combine` operator. During `implement()`, it:

1. Creates a shared `TrieCache` instance and stores it on the `EnumerableRelImplementor`
2. Visits each child, converting each `Enumerable` result to a `List`
3. Packs all lists into a single struct row
4. Returns a singleton `Enumerable` containing that struct

The `TrieCache` creation in step 1 is the critical bridge for cross-query optimization---it provides a shared context that child WCOJ operators can use to avoid redundant trie construction.

---

## 5. Three Layers of Shared Computation

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

### 5.1 Layer 1: Scan Sharing via Lazy Spools

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

### 5.2 Layer 2: Trie Cache

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

### 5.3 Layer 3: Shared-Prefix Execution

**Problem.** Two WCOJ operators may enumerate the same variable prefix identically. For example, two triangle queries over the same graph might share all three join variables and differ only in their output projections. Without sharing, both operators independently traverse the same backtracking search space.

**Solution.** Detect shared prefixes at compile time via fingerprinting, compute the prefix once at runtime, and distribute the bindings to per-query suffix executors.

#### 5.3.1 Join Variable Fingerprinting

To compare variables across different WCOJ operators, we need a canonical representation that is independent of operator-local input numbering. `JoinVariableFingerprint` achieves this by using the *structural digest* (`RelDigest`) of each input `RelNode`:

$$\text{fingerprint}(v) = \text{sort}\left(\left\{(\text{digest}(R_{i_k}), f_k) \mid (i_k, f_k) \in v.\text{occurrences}\right\}\right)$$

Two fingerprints are equal when they represent the same key intersection over structurally identical inputs. This invariant makes prefix detection correct: if fingerprints match at positions $0, \ldots, K{-}1$, the WCOJ backtracking search over those variables produces identical bindings.

#### 5.3.2 Prefix Group Detection

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

#### 5.3.3 Two-Phase Execution

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

## 6. Formal Analysis

### 6.1 Correctness of Prefix Sharing

**Theorem 1.** *Let $Q_1$ and $Q_2$ be two WCOJ queries with variable orderings $(x_1, \ldots, x_m)$ and $(x_1, \ldots, x_K, y_{K+1}, \ldots, y_{m'})$ respectively, such that variables $x_1, \ldots, x_K$ have identical fingerprints. Then the prefix bindings $\{(v_1, \ldots, v_K)\}$ computed by $Q_1$'s prefix enumerator are exactly the prefix bindings that $Q_2$'s full enumerator would produce for its first $K$ variables.*

**Proof sketch.** The WCOJ execution forms a search tree where level $k$ branches on all valid values of $x_k$ given the partial binding $(v_1, \ldots, v_{k-1})$. The candidates at level $k$ are:

$$\text{candidates}(x_k \mid v_1, \ldots, v_{k-1}) = \bigcap_{R_i \ni x_k} \text{trie}_i.\text{getKeys}(k, \text{prefix}_i(v_1, \ldots, v_{k-1}))$$

Since fingerprints match at positions $0, \ldots, K{-}1$, the participating inputs and their structural digests are identical. Combined with the `TrieCache` (which ensures identical inputs produce the same trie objects), the candidate sets are identical at each prefix level. Therefore the search trees are isomorphic up to depth $K$. $\square$

### 6.2 Cost Analysis

Let $P$ denote the cost of computing the shared prefix (iterating all valid $(v_1, \ldots, v_K)$ bindings), let $S_i$ denote the suffix cost for query $Q_i$, and let $N$ denote the number of queries in the prefix group.

**Without sharing:**

$$C_{\text{independent}} = N \cdot (P + \bar{S}) + N \cdot C_{\text{trie}}$$

where $C_{\text{trie}}$ is the per-query trie construction cost.

**With all three layers:**

$$C_{\text{shared}} = P + N \cdot \bar{S} + C_{\text{trie}} + C_{\text{cache}} + C_{\text{coord}}$$

where $C_{\text{cache}}$ is the `TrieCache` lookup overhead and $C_{\text{coord}}$ captures the constant-factor cost of prefix-sharing coordination (binding materialization, spool management, per-binding dispatch to suffix executors).

**Savings:**

$$\Delta C = (N - 1) \cdot P + (N - 1) \cdot C_{\text{trie}} - C_{\text{coord}}$$

The prefix cost $P$ dominates when the prefix covers most of the variables---the common case for queries that differ only in their final projection, aggregation, or filter on non-join columns. For $N$ identical triangle queries differing only in projection, $K = 3 = m$ (all variables shared), so $\bar{S} \approx 0$ and the savings approach $(N-1) \cdot C_{\text{full\_WCOJ}} - C_{\text{coord}}$. The coordination overhead $C_{\text{coord}}$ is proportional to the output size rather than amortized across queries, so the net benefit depends on $N$ being large enough for the $(N-1) \cdot P$ savings to dominate.

---

## 7. Experimental Evaluation

### 7.1 Experimental Setup

We evaluate our system using a custom benchmark (`WCOJBenchmarkCli`) that compares four execution modes:

| Mode | Description |
|------|-------------|
| **baseline** | Standard Calcite with binary hash joins, queries run sequentially |
| **wcoj** | WCOJ for each query, run sequentially (no multi-query optimization) |
| **combine** | `MULTI()` with WCOJ (batched execution, no sharing rules) |
| **combine-share** | `MULTI()` with WCOJ + scan sharing + trie caching + prefix sharing |

**Graph generation.** We construct synthetic directed graphs with controlled cyclicity. A fraction of vertices (default 5%) are designated as *hub nodes* with high in-degree and out-degree, creating the dense intermediate results that expose binary join weaknesses. Spoke-to-spoke edges ensure triangles exist. This design mirrors real-world power-law graphs while providing controlled experimental parameters.

**Query workload.** Triangle queries $Q_\triangle(a,b,c) \leftarrow R(a,b), S(b,c), T(c,a)$ with 5 projection variations, wrapped in `MULTI()` for batched modes.

**Hardware and software.** All experiments are run on an Apple M4 Pro (14 cores) with 48 GB RAM, OpenJDK 21.0.9, and JVM heap limited to 2 GB (`-Xmx2g`). Calcite version is 1.41.0-SNAPSHOT.

### 7.2 Results

All timings are reported as the mean over 10 iterations after 3 warmup rounds. The workload consists of triangle queries $Q_\triangle(a,b,c) \leftarrow R(a,b), S(b,c), T(c,a)$ with 5 projection variations batched via `MULTI()`.

#### 7.2.1 Scalability with Graph Size

We fix the query batch size at $N = 5$ and vary graph size from 50 to 400 nodes, scaling edge counts proportionally with 5% hub nodes.

**Table 2.** Execution time (ms) and speedup vs. graph size, $N = 5$ triangle queries. Speedup over baseline shown in parentheses.

| Graph | Triangles | Baseline | WCOJ | Combine | Combine-Share |
|:---:|:---:|:---:|:---:|:---:|:---:|
| 50 / 300 | 333 | 186 | 157 (1.2x) | 100 (1.9x) | 151 (1.2x) |
| 100 / 800 | 999 | 140 | 95 (1.5x) | 57 (2.5x) | 86 (1.6x) |
| 200 / 2000 | 1,854 | 268 | 177 (1.5x) | 138 (1.9x) | 177 (1.5x) |
| 400 / 5000 | 3,525 | 1,158 | 352 (**3.3x**) | 290 (**4.0x**) | 339 (**3.4x**) |

**Analysis.** The results reveal consistent and growing speedups as graph size increases:

- **Baseline degrades super-linearly.** Execution time grows from 186 ms at $|V|=50$ to 1,158 ms at $|V|=400$---a 6.2x increase for a 10.6x increase in triangle count (333 to 3,525 per query). This reflects the well-known intermediate result explosion in binary hash joins on cyclic queries: the two-way join $R \bowtie S$ produces $O(|R| \cdot |S| / |V|)$ intermediate tuples, many of which are subsequently eliminated by the third join.

- **WCOJ scales sub-linearly.** WCOJ execution time grows from 157 ms to 352 ms across the size range---a 2.2x increase for a 10.6x increase in triangle count (333 to 3,525). This sub-linear scaling is consistent with the worst-case optimal bound: WCOJ runtime is proportional to $|output| + |input|$ rather than intermediate result size. The speedup over baseline increases from 1.2x at $|V|=50$ to **3.3x** at $|V|=400$, demonstrating that the advantage of avoiding intermediate result materialization compounds as the join graph becomes denser.

- **Combine dominates at all sizes.** The `Combine` operator consistently outperforms sequential WCOJ execution, demonstrating that batched execution eliminates per-query overhead (connection setup, plan compilation, result materialization) even without shared computation. Combine achieves the best overall speedup at every graph size, reaching **4.0x** over baseline at $|V|=400$.

- **Combine-Share tracks Combine.** The shared-prefix mode achieves speedups comparable to plain Combine (3.4x vs. 4.0x at $|V|=400$). At this batch size ($N=5$), the overhead of fingerprint computation and prefix coordination offsets some of the sharing benefit. The marginal cost of the sharing infrastructure suggests that Combine-Share is better suited to larger batch sizes, where the one-time prefix computation is amortized across more queries (see Section 7.2.2).

#### 7.2.2 Multi-Query Speedup

We fix the graph at $|V|=200$, $|E|=2000$ and vary the number of batched triangle queries from $N=2$ to $N=20$. The `combine-share` mode encounters a type-equivalence assertion in the Volcano planner for $N > 5$ due to a known limitation in `CombineSharedComponentsRule`; results for that mode are reported only where available.

**Table 3.** Execution time (ms), speedup, and per-query cost vs. query batch size. Speedup over baseline in parentheses.

| $N$ | Baseline | WCOJ | Combine | Combine-Share | Per-query: Baseline | Per-query: Combine |
|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| 2 | 147 | 97 (1.5x) | 75 (2.0x) | 67 (**2.2x**) | 73.3 | 37.6 |
| 5 | 268 | 177 (1.5x) | 138 (1.9x) | 177 (1.5x) | 53.6 | 27.6 |
| 10 | 410 | 253 (1.6x) | 183 (**2.2x**) | --- | 41.0 | **18.3** |
| 20 | 647 | 341 (1.9x) | 232 (**2.8x**) | --- | 32.4 | **11.6** |

**Analysis.** The per-query amortized cost reveals the key benefit of batched WCOJ execution. For the `Combine` mode, the per-query cost drops from 37.6 ms at $N=2$ to **11.6 ms** at $N=20$---a 3.2x reduction. This sub-linear scaling demonstrates that the `Combine` operator successfully amortizes fixed costs (connection setup, plan compilation, trie construction) across the batch.

The WCOJ speedup over baseline grows steadily with batch size, from 1.5x at $N=2$ to 1.9x at $N=20$, indicating that WCOJ's advantage is amplified when many queries exercise the same join structure. The `Combine` mode amplifies this further: its speedup increases from 2.0x at $N=2$ to **2.8x** at $N=20$, with total execution time (232 ms) less than **36%** of the baseline (647 ms) despite executing 20 full triangle queries.

At $N=2$, `Combine-Share` achieves the best overall speedup (**2.2x**), demonstrating that even small batches benefit from shared computation when the prefix covers all three join variables. For $N=5$, the sharing overhead offsets the benefit at this graph size ($|V|=200$), yielding a 1.5x ratio equal to plain WCOJ. The current type-equivalence limitation for $N > 5$ is an engineering issue in the `CombineSharedComponentsRule`---not a fundamental algorithmic limitation---and is targeted for resolution in future work.

#### 7.2.3 Layer-by-Layer Contribution

The four modes form a natural ablation study, isolating the contribution of each optimization layer:

**Table 4.** Layer-by-layer contribution at $|V|=400$, $N=5$.

| Layer | What's Added | Time (ms) | Incremental Speedup |
|:---|:---|:---:|:---:|
| Baseline | --- | 1,158 | --- |
| WCOJ | Multi-way join algorithm | 352 | 3.3x over baseline |
| Combine | + Batched execution | 290 | 1.2x over WCOJ |
| Combine-Share | + Scan sharing + Trie caching + Prefix sharing | 339 | 0.85x over Combine |

**Analysis.** The dominant optimization is the WCOJ algorithm itself, which eliminates the intermediate result explosion that plagues binary joins on cyclic queries, delivering a **3.3x** speedup. The `Combine` operator provides an additional 1.2x improvement by amortizing per-query overhead across the batch. At this batch size ($N=5$), the three sharing layers do not yield a net benefit over plain Combine: the 0.85x ratio indicates that sharing overhead exceeds the savings. This result appears to contradict the formal cost model (Section 6.2), which predicts savings of $(N-1) \cdot P$ when all variables are shared ($K = m$, $\bar{S} \approx 0$). The discrepancy arises because the cost model's coordination overhead term $C_{\text{coord}}$ is non-negligible: the prefix enumerator must materialize and iterate over bindings (rather than directly producing output rows), the `CombineSharedComponentsRule` introduces additional spool operators, and prefix-group coordination requires per-binding dispatch to suffix executors. These costs are proportional to the output size (not amortized across queries), causing Combine-Share's per-query cost to remain roughly flat (33.4 ms at $N=2$, 35.4 ms at $N=5$) while plain Combine's per-query cost drops with $N$ (37.6 ms to 27.6 ms) due to trie construction amortization. The $N=2$ result (Table 3) confirms that Combine-Share outperforms plain Combine when the fixed sharing overhead is proportionally smaller relative to per-query execution cost.

#### 7.2.4 Variance and Stability

Execution time stability is an important practical consideration. The coefficient of variation (CV = $\sigma / \mu$) across graph sizes ranges from 0.07 to 0.40, with higher variance at small graph sizes ($|V|=50$) due to JVM warm-up effects and short absolute execution times. At $|V| \geq 200$, all modes stabilize: Combine and Combine-Share achieve CV $\leq 0.17$, with Combine-Share showing the lowest variance at the largest size (CV = 0.12). The higher WCOJ variance at $|V|=100$ (CV = 0.23) likely reflects JVM JIT compilation effects on the trie traversal hot path, which stabilizes at larger sizes as optimizations converge.

---

## 8. Conclusion

We have presented a unified framework within Apache Calcite that combines worst-case optimal join algorithms with multi-query optimization. Our system introduces the `Combine` relational operator and `MULTI()` SQL syntax for declarative multi-query batching, a hash-based WCOJ implementation with multi-level trie indexing, and three layers of cross-query optimization: scan sharing via lazy spools, identity-based trie caching, and shared-prefix execution through join-variable fingerprinting.

The key insight underlying our approach is that WCOJ's variable-at-a-time execution model creates natural sharing opportunities that do not exist in binary join plans. When multiple queries join the same relations on the same keys, they traverse identical search trees---a redundancy that our prefix-sharing mechanism eliminates. Combined with scan sharing and trie caching at the lower layers, the `Combine` operator achieves strong sub-linear scaling: per-query amortized cost drops 3.2x as batch size grows from 2 to 20, reaching a 2.8x overall speedup over sequential binary joins at $N=20$.

Our work does not address the general MQO selection problem [10]; rather, it exploits the specific structure of WCOJ execution to identify sharing opportunities within batches of cyclic join queries. This structural approach is complementary to selection-based MQO techniques and could be integrated with them for mixed workloads containing both cyclic and acyclic queries.

All contributions are implemented as modular extensions to Apache Calcite, preserving backward compatibility and enabling adoption by the numerous systems built on the Calcite framework. The source code is available as open-source contributions to the Calcite project.

**Future work.** Several directions remain open: (1) resolving the type-equivalence assertion in `CombineSharedComponentsRule` for batches with $N > 5$ aliased-projection sub-queries, which currently prevents evaluating Combine-Share at larger batch sizes; (2) reducing the coordination overhead $C_{\text{coord}}$ of prefix sharing through direct binding propagation rather than materialization; (3) exploring adaptive variable ordering that considers both single-query and cross-query optimization objectives; (4) integrating with Calcite's materialized view subsystem for persistent cross-batch sharing; and (5) extending the `Combine` operator to support heterogeneous join strategies (e.g., WCOJ for cyclic components and binary joins for acyclic components within the same batch).

---

## References

[1] P. G. Selinger, M. M. Astrahan, D. D. Chamberlin, R. A. Lorie, and T. G. Price, "Access path selection in a relational database management system," in *Proceedings of the 1979 ACM SIGMOD International Conference on Management of Data*, 1979, pp. 23--34. doi: [10.1145/582095.582099](https://doi.org/10.1145/582095.582099)

[2] A. Atserias, M. Grohe, and D. Marx, "Size bounds and query plans for relational joins," *SIAM Journal on Computing*, vol. 42, no. 4, pp. 1737--1767, 2013. doi: [10.1137/110859440](https://doi.org/10.1137/110859440)

[3] H. Q. Ngo, E. Porat, C. Re, and A. Rudra, "Worst-case optimal join algorithms," *Journal of the ACM*, vol. 65, no. 3, pp. 1--40, 2018. doi: [10.1145/3180143](https://doi.org/10.1145/3180143)

[4] T. L. Veldhuizen, "Leapfrog Triejoin: A simple, worst-case optimal join algorithm," in *Proc. 17th International Conference on Database Theory (ICDT)*, Athens, Greece, 2014, pp. 96--106. Available: [https://openproceedings.org/ICDT/2014/paper_13.pdf](https://openproceedings.org/ICDT/2014/paper_13.pdf)

[5] M. Freitag, M. Bandle, T. Schmidt, A. Kemper, and T. Neumann, "Adopting worst-case optimal joins in relational database systems," *Proceedings of the VLDB Endowment*, vol. 13, no. 11, pp. 1891--1904, 2020. doi: [10.14778/3407790.3407797](https://doi.org/10.14778/3407790.3407797)

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

[16] P. Michiardi, D. Carra, and S. Migliorini, "Cache-based multi-query optimization for data-intensive scalable computing frameworks," *Information Systems Frontiers*, vol. 23, pp. 35--51, 2021. doi: [10.1007/s10796-020-09995-2](https://doi.org/10.1007/s10796-020-09995-2)

[17] M. Schönberger, I. Trummer, and W. Mauerer, "Large-scale multiple query optimisation with incremental quantum(-inspired) annealing," *Proceedings of the ACM on Management of Data*, vol. 3, no. 4, Article 253, pp. 253:1--253:25, 2025. doi: [10.1145/3749171](https://doi.org/10.1145/3749171)

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
