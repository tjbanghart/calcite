# Worst-Case Optimal Joins with Multi-Query Reuse

## Shared Components for WCOJ in Apache Calcite

---

## 1. Introduction

Traditional relational databases process joins pairwise: a tree of binary hash joins where each node produces an intermediate result fed into the next. For acyclic query graphs (stars, snowflakes, chains), this works well — the optimizer can pick a join order that keeps intermediate results small. But for **cyclic** queries — triangles, cliques, and other graph patterns — binary join plans can produce intermediate results exponentially larger than the final output.

**Worst-Case Optimal Join (WCOJ)** algorithms solve this. Instead of joining two relations at a time, WCOJ binds variables one at a time, intersecting candidate values across all participating relations simultaneously. The resulting runtime is bounded by the **AGM bound** (Atserias, Grohe, Marx 2008) — the information-theoretic worst-case output size — rather than by the size of intermediate results that may never appear in the output.

Our implementation in Apache Calcite goes further: when multiple queries share the same cyclic join structure, we **reuse computation** across them. This document describes the three layers of shared components we built and how they connect to the mathematical foundations in Veldhuizen's Leapfrog Triejoin and Freitag et al.'s adoption of WCOJ into cost-based optimizers.

---

## 2. Background: From Paper to Algorithm

### 2.1 The Binary Join Problem on Cyclic Queries

Consider the triangle query — find all directed triangles `a → b → c → a` in a graph:

```sql
SELECT e1.src, e1.dst, e2.dst
FROM edges e1, edges e2, edges e3
WHERE e1.dst = e2.src      -- a → b, b → ...
  AND e2.dst = e3.src      -- b → c, c → ...
  AND e3.dst = e1.src      -- c → a (closes the cycle)
```

A binary join plan first computes `e1 ⋈ e2` on `e1.dst = e2.src`, producing all 2-hop paths `(a, b, c)`. Then it joins with `e3` on `e2.dst = e3.src AND e3.dst = e1.src`. The problem: the intermediate result `e1 ⋈ e2` can be `O(|E|^2)` for dense hub graphs — far larger than the actual triangle count.

### 2.2 Veldhuizen's Leapfrog Triejoin

Veldhuizen (2014) introduced Leapfrog Triejoin, where the core idea is **variable-at-a-time enumeration with trie intersection**:

> *"Rather than joining two relations at a time, we process one variable at a time. For each variable, we intersect the candidate values across all relations that constrain it."*

The pseudocode from the paper, adapted to our notation:

```
GENERIC-JOIN(Relations R₁...Rₙ, Variables x₁...xₘ):
    if m = 0:
        yield cross-product of matching tuples
        return

    // Intersect candidate values for x₁ across all relations that mention x₁
    candidates ← ∩{π_{x₁}(Rᵢ | current bindings) : Rᵢ mentions x₁}

    for each value v in candidates:
        bind x₁ ← v
        GENERIC-JOIN(R₁...Rₙ, x₂...xₘ)  // recurse on remaining variables
```

The key operations are:
1. **Build** a multi-level trie (hash index) per relation, with levels ordered by the global variable ordering
2. **Probe** the trie with a prefix of already-bound variables to narrow candidates
3. **Intersect** candidate sets across all relations that participate in the current variable

### 2.3 Our Implementation: `WCOJEnumerator`

Our `WCOJEnumerator` (in `EnumerableDefaults.java`) maps directly to this pseudocode. The algorithm maintains:

| Paper Concept | Implementation | Location |
|---|---|---|
| Trie per relation | `HashTrie<Object[]> triePerInput[i]` | `EnumerableDefaults.java:5094` |
| Current variable bindings | `Object[] currentValues` | `EnumerableDefaults.java:5109` |
| Candidate values at level k | `List<Object> candidateValues[k]` | `EnumerableDefaults.java:5110` |
| Backtracking cursor | `int currentLevel` | `EnumerableDefaults.java:5117` |

The main loop in `moveNext()` implements iterative deepening with backtracking:

```
moveNext():
    // Phase 1: Initialize all levels (bind x₁, x₂, ..., xₘ)
    for level = 0 to numVariables - 1:
        initCandidatesAtLevel(level)    // intersect across relations
        advanceAtLevel(level)           // pick first candidate

    // Phase 2: Yield matches, then backtrack
    loop:
        collectMatches()                // probe tries with full binding → cross-product
        yield each match

        // Backtrack: find next valid assignment
        currentLevel--
        while currentLevel >= 0:
            if advanceAtLevel(currentLevel):   // try next candidate
                propagate forward...
                break
            else:
                currentLevel--                 // exhausted, backtrack further
```

The critical method is **`initCandidatesAtLevel(level)`** — this is where WCOJ's efficiency comes from:

```java
// For each input that participates in this variable:
for (int inputIdx : participatingInputs) {
    // Build prefix of prior variable bindings relevant to THIS input
    List<Object> prefix = new ArrayList<>();
    for (int priorLocal = 0; priorLocal < localLevel; priorLocal++) {
        int priorGlobalVar = inputVarOrder[inputIdx][priorLocal];
        prefix.add(currentValues[priorGlobalVar]);
    }

    // Query trie: "given these prior bindings, what values are possible here?"
    Set<Object> keys = trie.getKeysAtLevel(localLevel, prefix);

    // Intersect across all participating inputs
    intersection.retainAll(keys);
}
```

This is the direct implementation of the intersection step from Generic-Join. Each trie lookup uses the **prefix of already-bound variables** to navigate to the relevant subtrie, returning only values that are consistent with all prior bindings. The intersection across inputs ensures that only values satisfying **all** join conditions survive.

### 2.4 HashTrie: The Multi-Level Index

The `HashTrie` is a recursive hash map structure where each level corresponds to a join variable:

```
HashTrie for edges (levels: src, dst):
    root
    ├── src=1 → {dst=2: [row(1,2)], dst=5: [row(1,5)]}
    ├── src=2 → {dst=3: [row(2,3)]}
    └── src=3 → {dst=1: [row(3,1)]}
```

Key operations:
- **`build(source, keyExtractors)`** — scans the input once, inserting each row into the trie by extracting keys at each level
- **`getKeysAtLevel(level, prefix)`** — navigates using the prefix, returns distinct keys at the target level
- **`probe(keyValues)`** — full probe with all levels specified, returns matching rows

The trie structure is what makes WCOJ efficient: instead of materializing all `(a, b)` pairs from the first join and then filtering, we navigate directly to valid `b` values given the current `a` binding, and intersect with valid `b` values from the third relation.

### 2.5 Cyclic Detection and Rule Firing

Not all multi-way joins benefit from WCOJ. For acyclic (tree-shaped) join graphs, binary join plans with good ordering are optimal. WCOJ provides its advantage specifically on **cyclic** queries.

`EnumerableWCOJRule` detects this:

1. **Flatten**: `JoinToMultiJoinRule` converts a tree of `LogicalJoin` nodes into a single `MultiJoin`
2. **Extract join graph**: Equi-join conditions are parsed, and a Union-Find groups columns into equivalence classes. Each class spanning 2+ inputs becomes a `JoinVariable`
3. **Cycle test**: Build an adjacency graph between inputs (edges from shared variables). A connected graph with `edges >= nodes` contains a cycle (a tree has exactly `nodes - 1` edges)

Only when the join graph is cyclic does the rule fire, converting `MultiJoin → EnumerableWCOJ`.

---

## 3. Shared Components Architecture

The shared components form three layers, each addressing a different level of redundancy when multiple WCOJ queries execute together inside a `Combine` operator:

```
┌─────────────────────────────────────────────────────┐
│              Layer 3: Prefix Sharing                 │
│  EnumerableCombineWcojPrefixRule                     │
│  WCOJPrefixAnalyzer + JoinVariableFingerprint        │
│  "Compute shared variable prefix once, reuse suffix" │
├─────────────────────────────────────────────────────┤
│              Layer 2: Trie Cache                     │
│  TrieCache (IdentityHashMap-based)                   │
│  "Build each trie once, share across WCOJ operators" │
├─────────────────────────────────────────────────────┤
│              Layer 1: Scan Sharing                   │
│  CombineSharedComponentsRule                         │
│  RelCommonExpressionBasicSuggester + Spools          │
│  "Materialize shared scans once via lazy spools"     │
└─────────────────────────────────────────────────────┘
```

### 3.1 Layer 1: Scan Sharing via Spools

**Problem**: When multiple queries inside a `Combine` scan the same table, each scan reads the data independently — doubling (or tripling, etc.) the I/O.

**Solution**: `CombineSharedComponentsRule` detects structurally identical subtrees across Combine inputs and replaces them with a **spool** (temporary materialization).

**Algorithm**:

1. Use `RelCommonExpressionBasicSuggester` to find all subtrees that appear in multiple Combine inputs (compared by `RelDigest` — structural equivalence)
2. For each shared subtree:
   - Create a `SpoolRelOptTable` backed by in-memory storage
   - Wrap it in a `LogicalTableSpool` (the producer) with `LAZY` read/write semantics
3. Replace occurrences using a `RelHomogeneousShuttle`:
   - **First occurrence** → the spool (produces data, writes to storage)
   - **Subsequent occurrences** → `LogicalTableScan` on the spool table (reads from storage)

**Example**: Two triangle queries over the same graph both scan `edges`. After this rule:

```
Before:                          After:
Combine                          Combine
├── WCOJ(scan_e1, scan_e2, ...)  ├── WCOJ(SPOOL(scan_e1), read_spool1, ...)
└── WCOJ(scan_e1, scan_e2, ...)  └── WCOJ(read_spool1, read_spool2, ...)
```

The spool ensures the table is scanned exactly once. This is the foundation for Layer 2 — the `TrieCache` relies on object identity to detect shared inputs, and spools provide exactly that: multiple consumers reading from the same materialized data object.

### 3.2 Layer 2: Trie Cache

**Problem**: Even after scan sharing, each WCOJ operator independently builds a `HashTrie` from its inputs. If two WCOJs join the same relation on the same key, they build the same trie twice.

**Solution**: `TrieCache` — a per-execution cache of `HashTrie` instances, shared across all WCOJ operators within a `Combine`.

```java
public class TrieCache {
    // Two-level cache: input identity → field index → HashTrie
    private final IdentityHashMap<Enumerable<Object[]>,
                                  Map<Integer, HashTrie<Object[]>>> cache;

    public HashTrie<Object[]> getOrBuild(Enumerable<Object[]> input, int fieldIdx) {
        return cache
            .computeIfAbsent(input, k -> new HashMap<>())
            .computeIfAbsent(fieldIdx, k -> HashTrie.build(input, extractors));
    }
}
```

The design is deliberately simple:
- **Identity-based lookup**: Uses `IdentityHashMap`, so sharing only occurs when inputs are the same Java object reference (not just structurally equivalent). This is sound because Layer 1's spools ensure shared scans produce the same object
- **Lazy building**: `getOrBuild` computes on first access, subsequent calls return the cached trie
- **Lifecycle**: Created by `EnumerableCombine.implement()`, passed to all child WCOJ operators, cleared after execution

**Connection to the paper**: Veldhuizen's algorithm builds tries per relation at the start of execution. The TrieCache extends this to the multi-query setting: instead of each query building its own tries, the cache amortizes construction across all queries in the Combine.

### 3.3 Layer 3: Prefix Sharing

**Problem**: Two WCOJ operators may enumerate the same variable prefix (the first K variables) identically. For example, two triangle queries over the same graph might share the first two variables and only diverge at the third. Without sharing, both operators independently iterate all `(x₁, x₂)` bindings using the same backtracking search.

**Solution**: Detect shared prefixes via fingerprinting, compute the prefix once, and share the bindings.

This is the most algorithmically interesting layer and the one with the deepest connection to the WCOJ paper.

#### 3.3.1 Fingerprinting: `JoinVariableFingerprint`

To compare variables across different WCOJ operators, we need a canonical representation. A `JoinVariable` has a list of `(inputIndex, fieldIndex)` occurrences — but input indices are local to each WCOJ operator. Two variables in different WCOJs might refer to the same physical join work but have different input numbering.

`JoinVariableFingerprint` solves this by using the **structural digest** of each input `RelNode`:

```java
public static JoinVariableFingerprint create(JoinVariable variable, List<RelNode> inputs) {
    List<InputFieldRef> refs = new ArrayList<>();
    for (Pair<Integer, Integer> occ : variable.occurrences) {
        String digest = inputs.get(occ.left).getRelDigest().toString();
        refs.add(new InputFieldRef(digest, occ.right));
    }
    refs.sort(canonicalOrder);  // Sort by (digest, fieldIndex)
    return new JoinVariableFingerprint(refs);
}
```

Two fingerprints are equal when they represent the same key intersection over structurally identical inputs. This is the invariant that makes prefix detection correct: if fingerprints match at positions 0..K-1, the WCOJ backtracking search over those variables is identical.

#### 3.3.2 Prefix Detection: `WCOJPrefixAnalyzer`

Given N WCOJ operators, the analyzer builds a **trie of fingerprint sequences** to find shared prefixes:

```
Input: WCOJ₀ with variables [fp_A, fp_B, fp_C]
       WCOJ₁ with variables [fp_A, fp_B, fp_D]
       WCOJ₂ with variables [fp_X, fp_Y]

Prefix trie:
    root
    ├── fp_A → fp_B → ┬── fp_C  (WCOJ₀)
    │                  └── fp_D  (WCOJ₁)
    └── fp_X → fp_Y            (WCOJ₂)

Divergence at depth 2 for {WCOJ₀, WCOJ₁}
→ PrefixGroup(depth=2, members=[0, 1])
```

The `extractGroups` traversal finds divergence points: nodes where `children.size() > 1`. All queries reachable from that node share the prefix up to that depth.

#### 3.3.3 Execution: Prefix + Suffix Factoring

Once prefix groups are detected, `EnumerableCombineWcojPrefixRule` wraps each WCOJ in an `EnumerableWCOJWithPrefix`, which generates code for two-phase execution:

**Phase 1 — Prefix computation** (`WCOJPrefixEnumerator`):
```
WCOJ-PREFIX(Relations R₁...Rₙ, Variables x₁...xₖ):  // k = prefixDepth
    Build tries for all inputs
    for each valid binding of (x₁, ..., xₖ):       // standard WCOJ backtracking
        yield (x₁, ..., xₖ)                         // output binding, not rows
```

This is the standard WCOJ algorithm truncated at depth K. It yields variable bindings, not result rows.

**Phase 2 — Suffix execution** (`WCOJWithPrefixEnumerator`):
```
WCOJ-WITH-PREFIX(Relations, Variables, prefixBindings, prefixDepth):
    for each binding (x₁, ..., xₖ) from prefixBindings:
        resetWithPrefix(binding, k)
        // Set x₁...xₖ, initialize suffix variables xₖ₊₁...xₘ
        while moveNextSuffix():    // backtrack only within xₖ₊₁...xₘ
            yield result row
```

The critical method is `moveNextSuffix()`, which uses a **floor** on backtracking:

```java
boolean moveNextSuffix() {
    final int floor = suffixPrefixDepth;
    while (currentLevel >= floor) {    // never backtrack below the prefix
        if (advanceAtLevel(currentLevel)) {
            propagate forward...
        } else {
            currentLevel--;
        }
    }
    if (currentLevel < floor) return false;  // exhausted this prefix binding
}
```

This is a direct decomposition of the WCOJ recursion. The paper's `GENERIC-JOIN` recurses over all variables; we split it into prefix recursion (shared) and suffix recursion (per-query), with the boundary at the first divergence point.

#### 3.3.4 Mathematical Justification

The correctness of prefix sharing follows from the structure of Generic-Join. The algorithm's execution forms a **search tree** where:
- Level 0 branches on all valid values of x₁
- Level 1 branches (under each x₁ binding) on all valid values of x₂
- ...and so on

If two queries Q₁ and Q₂ have identical variables at levels 0..K-1 over the same relations, their search trees are identical up to depth K. Sharing the prefix means traversing this common subtree once instead of twice.

**Cost analysis**: Let `P` be the cost of computing the prefix (iterating all valid `(x₁, ..., xₖ)` bindings), and let `S_i` be the cost of the suffix for query `Q_i`. Without sharing:

```
Total cost = K × (P + S_avg)           // each query pays full cost
```

With sharing:

```
Total cost = P + K × S_avg + T_cache   // prefix computed once
```

where `T_cache` is the TrieCache overhead (negligible — hash map lookups). The savings are `(K-1) × P`, which is significant when the prefix covers most of the variables (the common case for queries that differ only in their final projection or aggregation).

---

## 4. How It All Composes

### 4.1 End-to-End Flow

Consider a workload: 5 triangle queries over the same graph, differing only in which columns they project.

```sql
MULTI(
  (SELECT e1.src, e1.dst, e2.dst FROM edges ...triangle...),
  (SELECT e1.src, e2.src, e3.src FROM edges ...triangle...),
  (SELECT e1.src, e2.dst, e3.weight FROM edges ...triangle...),
  (SELECT e1.src, e1.dst, e2.dst, e3.weight FROM edges ...triangle...),
  (SELECT e3.dst, e1.src, e2.src, e1.weight FROM edges ...triangle...)
)
```

**Step 1: Parsing**
`MULTI(...)` is parsed into a `Combine` with 5 sub-queries.

**Step 2: Logical optimization**
Each sub-query goes through standard Calcite optimization. `JoinToMultiJoinRule` flattens each 3-way join tree into a `MultiJoin`.

**Step 3: Physical conversion** (`EnumerableWCOJRule`)
Each `MultiJoin` is detected as cyclic (triangle = 3 nodes, 3 edges ≥ 3 nodes) and converted to `EnumerableWCOJ`. Each WCOJ has 3 join variables:
- Var0: `e1.dst = e2.src` (the `b` node)
- Var1: `e2.dst = e3.src` (the `c` node)
- Var2: `e3.dst = e1.src` (closing the triangle back to `a`)

**Step 4: Layer 1** (`CombineSharedComponentsRule`)
Detects that all 5 WCOJs scan the same edge tables. Introduces spools so each table is scanned once.

**Step 5: Layer 3** (`EnumerableCombineWcojPrefixRule`)
`WCOJPrefixAnalyzer` computes fingerprints for each WCOJ's variables. Since all 5 queries join the same tables on the same keys, all 3 variables have identical fingerprints. Result: one `PrefixGroup(depth=3, members=[0,1,2,3,4])` — all variables are shared.

Each WCOJ is wrapped in `EnumerableWCOJWithPrefix(prefixDepth=3)`.

**Step 6: Code generation** (`EnumerableCombine.implement()`)
- Creates a shared `TrieCache` (Layer 2)
- For the first WCOJ: generates `wcojPrefix()` call (iterates all triangle bindings)
- For each WCOJ: generates `wcojWithSharedPrefix()` call (reuses prefix bindings, only applies its projection)

**Step 7: Runtime execution**
1. Tries are built once (cached in TrieCache)
2. The prefix enumerator finds all `(a, b, c)` triangle bindings once
3. Each query's suffix enumerator receives these bindings and projects its specific columns
4. All 5 result sets are collected into the Combine output

### 4.2 Optimization Layers Summary

| Layer | What's Shared | Mechanism | Savings |
|---|---|---|---|
| **1. Scan Sharing** | Table reads | Spools (materialize once, read many) | Avoids N redundant full-table scans |
| **2. Trie Cache** | Index structures | Identity-based hash map cache | Avoids N redundant trie builds from same data |
| **3. Prefix Sharing** | Backtracking search | Factored prefix/suffix execution | Avoids N redundant traversals of shared search space |

---

## 5. Key Files

| Component | File | Lines |
|---|---|---|
| WCOJ Operator | `core/.../enumerable/EnumerableWCOJ.java` | ~340 |
| WCOJ Rule (cyclic detection) | `core/.../enumerable/EnumerableWCOJRule.java` | ~333 |
| WCOJ Runtime (enumerators) | `linq4j/.../EnumerableDefaults.java` | 4983–5568 |
| HashTrie | `linq4j/.../HashTrie.java` | ~230 |
| TrieCache | `linq4j/.../TrieCache.java` | ~69 |
| Combine Operator | `core/.../enumerable/EnumerableCombine.java` | ~112 |
| Scan Sharing Rule | `core/.../rules/CombineSharedComponentsRule.java` | ~198 |
| Prefix Sharing Rule | `core/.../enumerable/EnumerableCombineWCOJPrefixRule.java` | ~138 |
| Prefix Analyzer | `core/.../enumerable/WCOJPrefixAnalyzer.java` | ~158 |
| Variable Fingerprint | `core/.../enumerable/JoinVariableFingerprint.java` | ~116 |
| WCOJ With Prefix | `core/.../enumerable/EnumerableWCOJWithPrefix.java` | ~279 |
| Benchmark | `core/.../test/WCOJBenchmarkCli.java` | ~700 |

---

## 6. References

1. **Veldhuizen, T.L.** "Leapfrog Triejoin: A Simple, Worst-Case Optimal Join Algorithm." *ICDT 2014.* https://www.openproceedings.org/2014/conf/icdt/Veldhuizen14.pdf

2. **Freitag, M.J. et al.** "Adopting Worst-Case Optimal Joins in Relational Database Systems." *VLDB 2020.* https://db.in.tum.de/~freitag/papers/p1891-freitag.pdf

3. **Atserias, A., Grohe, M., Marx, D.** "Size bounds and query plans for relational joins." *SIAM Journal on Computing, 2013.* (The AGM bound.)

4. **Ngo, H.Q., Porat, E., Ré, C., Rudra, A.** "Worst-case Optimal Join Algorithms." *JACM 2018.* (Theoretical foundations for Generic-Join.)
