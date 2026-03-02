# Peer Review: "Worst-Case Optimal Joins Meet Multi-Query Optimization: Shared Computation in Apache Calcite"

**Reviewer:** Anonymous PhD Reviewer
**Date:** 2026-02-23
**Recommendation:** Major Revision Required

---

## Summary

This paper presents an integration of worst-case optimal join (WCOJ) algorithms with multi-query optimization (MQO) in Apache Calcite. The core contribution is the `Combine` relational operator and `MULTI()` SQL syntax, supported by three cross-query optimizations: identity-based trie caching, sub-expression sharing via spools, and shared-prefix execution. The paper is well-organized, the related work is thorough, and the implementation appears technically sound. However, the experimental evaluation contains several serious issues—including omitted results that are unfavorable to the system, a methodological inconsistency in iteration count, and inadequate statistical analysis—that prevent acceptance in their current form.

---

## Critical Issues

### 1. Selective Reporting: Omitted Benchmark Results Where WCOJ Is Significantly Slower

This is the most serious concern. The repository contains a benchmark file (`tpch-wcoj-sf001.csv`) with 10-iteration results for three FK (foreign-key) join shapes on TPC-H data: triangle, rectangle, and diamond. None of these appear in Table 5 or anywhere in the paper. Computing means from the raw data:

| Shape | Baseline (ms) | WCOJ (ms) | WCOJ/Baseline |
|-------|--------------|-----------|---------------|
| FK Triangle | 544 | 394 | **0.72 (1.38x faster)** |
| FK Rectangle | 860 | 1,314 | **1.53x SLOWER** |
| FK Diamond | 2,237 | 7,950 | **3.55x SLOWER** |

For the FK rectangle and FK diamond, WCOJ is dramatically worse than binary joins—1.5x and 3.5x slower, respectively. The diamond case is catastrophic: 2.2 seconds with binary joins versus nearly 8 seconds with WCOJ. These failures are not edge cases; they arise from the same TPC-H SF=0.01 dataset used for the reported results. Omitting them constitutes selective reporting of a material kind.

The paper does acknowledge that WCOJ is not universally beneficial ("WCOJ provides its primary advantage over binary joins precisely on cyclic query topologies," Section 3.2), but it does not explain *why* the FK rectangle and FK diamond fail so badly, nor does it characterize the conditions under which WCOJ regresses. This is a significant gap. A reviewer cannot assess the system's practical value without understanding these failure modes.

**Required action:** Include all benchmark results. Provide an analysis of why WCOJ underperforms on FK rectangle and FK diamond (likely due to high-cardinality intermediate expansions per variable level in the presence of FK structure). If these shapes expose fundamental limitations, say so clearly rather than omitting them.

---

### 2. Inconsistent Iteration Count for TPC-H 4-Cycle

Section 7.1 states: *"All timings are reported as the mean over 10 iterations after 3 warmup rounds."* However, the raw data for the TPC-H 4-cycle self-join (rectangle shape in `tpch-wcoj-selfjoin-sf001.csv`) contains only **5 iterations** for both baseline and WCOJ:

```
rectangle,baseline,1,...  through rectangle,baseline,5,...   (5 entries only)
rectangle,wcoj,1,...      through rectangle,wcoj,5,...        (5 entries only)
```

All other experiments have 10 iterations as claimed. The paper applies a uniform statistical claim to results that do not satisfy it. The mean of 5 samples is reported as equivalent to the mean of 10 samples, which it is not—especially when individual runs for the 4-cycle take 200+ seconds and variance may be high.

**Required action:** Collect the missing 5 iterations for the TPC-H 4-cycle, or explicitly qualify Table 5's 4-cycle row as being based on 5 iterations with a corresponding caveat.

---

### 3. `Combine` Is Slower Than Sequential WCOJ for TPC-H Triangle (Self-Join)

Table 5 reports the TPC-H self-join triangle as: WCOJ = 2,998 ms, Combine = 3,883 ms. Plain batching with `Combine` is **30% slower** than running queries sequentially with WCOJ—the opposite of the paper's central thesis. This result is buried in the text ("On the triangle queries, `Combine-Share` achieves the best overall performance...") without directly acknowledging that Combine regresses relative to WCOJ. A reader scanning Table 5 sees only speedups over baseline, obscuring the regression.

The paper partially attributes this pattern to coordination overhead but does not discuss it for the triangle case. `Combine-Share` (2,511 ms) does recover past WCOJ (2,998 ms), but the intermediate mode (plain `Combine`) harming performance is a counterintuitive result that deserves explicit discussion.

**Required action:** Add a column or annotation in Table 5 showing speedup relative to sequential WCOJ, not only relative to baseline. Discuss the conditions under which `Combine` alone degresses, and explain why Combine-Share recovers.

---

## Important Issues

### 4. No Statistical Analysis; Inadequate Handling of High Variance

The paper reports means without standard deviations, confidence intervals, or significance tests. For the N=2 batch-size experiment, the raw data shows:

- Combine: mean = 209.9 ms, **range = 68–380 ms, stdev = 111.4 ms** (CV ≈ 53%)
- Combine-Share: mean = 191.3 ms, **range = 81–371 ms, stdev = 95.3 ms** (CV ≈ 50%)

A coefficient of variation exceeding 50% means the reported mean is unreliable. The paper's statement that Combine achieves "1.1x" speedup at N=2 is not statistically meaningful given this variance. Similarly, for the FK-triangle in `tpch-wcoj-selfjoin-sf001.csv`, the WCOJ results range from 668 ms to 1,466 ms (2.2x range), yet a single mean of 882 ms is reported and rounded up to "1.1x speedup."

**Required action:** Report standard deviation or 95% confidence intervals for all key results. For the N=2 case, either explain the source of variance (JVM JIT, GC pressure, lazy compilation?) or collect more iterations until confidence intervals are meaningful. Mark results with CV > 20% explicitly.

---

### 5. "Super-Linear Degradation" Claim Is Not Supported by the Data

Section 7.2.1 states: *"The results reveal consistent and growing speedups... Baseline degrades super-linearly."* This claim is not supported. From Table 2:

- Baseline grows: 86 ms → 772 ms = **9.0x** increase
- Graph size grows: 50→400 nodes = 8x vertices, 300→5000 edges = **16.7x** edges
- Triangle count grows: 333→3525 = **10.6x** increase

Baseline execution time (9.0x) grows **slower** than both edge count (16.7x) and triangle count (10.6x). This is sub-linear relative to both the input and output size. The paper appears to conflate "growing faster than WCOJ" with "super-linear," which is not the same thing. To claim super-linearity, the paper must define the base quantity (edges? nodes? triangles?) and show that execution time grows as $O(n^k)$ for $k > 1$. The current data does not do this.

**Required action:** Replace "super-linearly" with a precise statement such as "Baseline execution time grows 9x while WCOJ grows 4.2x over the tested range," or provide a proper scaling analysis with log-log plots showing the growth exponents.

---

### 6. Cost Model Is Disconnected from Observed Performance

The cost model in Section 3.5 is:

$$C_{\text{WCOJ}} = \sum_{i=1}^{n} |R_i| + |Q(D)|$$

This model does not include the cost of failed candidate exploration (backtracking through dead ends), which is the dominant cost for the WCOJ failure cases. For the FK diamond, WCOJ is 3.55x slower than baseline, but the cost model predicts WCOJ should always be faster or equal (since $|Q(D)| \leq $ binary join intermediate sizes). A cost model that cannot predict or explain observed regressions is not useful for the optimizer.

The model also omits the per-level intersection cost, which is $O(|\text{keys at level } k|)$ per variable, not $O(|Q(D)|)$. For a query with a highly-fanout FK key (e.g., suppkey with ~600 lineitems per supplier), the intersection at that level explores many candidates even if most are eliminated—and this cost should appear in the model.

**Required action:** Extend the cost model to include the search/intersection cost, or clearly acknowledge that the current model is a lower bound that cannot predict regressions. Discuss the conditions under which the cost model breaks down (high fan-out FK joins, deep candidate sets with low selectivity).

---

### 7. Missing Ablation: Batched Binary Joins vs. Batched WCOJ

The four modes (baseline, wcoj, combine, combine-share) do not include a "combine-binary" mode: batched execution with `MULTI()` but using standard binary hash joins instead of WCOJ. Without this baseline, it is impossible to separate the contribution of WCOJ (multi-way join algorithm) from the contribution of batching and trie caching. It is plausible that a significant fraction of the reported speedup in `combine` over `baseline` comes from batch compilation amortization rather than from WCOJ itself.

From Table 3: at N=10, Combine achieves 3.0x over baseline, but WCOJ alone achieves 1.9x. The remaining 1.6x could be (a) trie caching, (b) batch compilation amortization, or (c) some combination. The paper attributes this to trie caching but does not prove it experimentally.

**Required action:** Add a "combine-binary" mode that uses `MULTI()` batching with standard binary joins. This would isolate the WCOJ contribution from the batching contribution.

---

## Moderate Issues

### 8. Correctness Proof Relies on Fragile Java Object Identity

**Theorem 1** and its proof rely on the `TrieCache`'s `IdentityHashMap` ensuring that "identical inputs produce the same trie objects." The proof states: *"Combined with the TrieCache (which ensures identical inputs produce the same trie objects), the candidate sets are identical at each prefix level."*

This is only true if the two WCOJ operators receive the *same Java object reference* for their shared inputs. If the same table is scanned by two separate `TableScan` operators (even with the same structural digest), they produce different `Enumerable` objects, and the `IdentityHashMap` will not find a cache hit. The correctness of prefix sharing would then be silently broken: the two operators would build separate tries with potentially different key ordering, making the fingerprint-based sharing invalid.

The paper should clarify: when does the `TrieCache` guarantee sharing? Is it only within a `Combine` node where child operators share the same physical scan object? If so, the correctness proof has a narrower scope than claimed.

**Required action:** Clarify the precise conditions under which `TrieCache` guarantees identity-based sharing. Ideally, prove that Calcite's code generation for `Combine` children guarantees shared object references for shared table inputs. If this is not guaranteed, the correctness proof is incomplete.

---

### 9. TPC-H Query Notation Is Confusing and May Be Incorrect

Section 7.2.4 writes the triangle query as:

$$Q_\triangle^{\text{TPC-H}}(o,s,p) \leftarrow L_1(\underline{o},s), L_2(\underline{s},o'), L_3(\underline{p},o'')$$

The head variables are $(o, s, p)$ but the body introduces $o'$ and $o''$ as distinct variables, suggesting the body is under-constrained (the head does not bind these free variables, creating a Cartesian product). The join conditions are given separately in a `where` clause, which is not standard Datalog notation. A properly written Datalog rule for a lineitem triangle on orderkey/suppkey/partkey would be:

$$Q_\triangle(l_1, l_2, l_3) \leftarrow \text{lineitem}(l_1, o, s_1, p_1, \ldots), \text{lineitem}(l_2, o, s_2, p_2, \ldots), \text{lineitem}(l_3, o, s_3, p_3, \ldots)$$

or equivalently with explicit join variable equalities. The current notation obscures the join structure and should be rewritten for clarity.

**Required action:** Rewrite the TPC-H query in standard Datalog or SQL notation that clearly identifies the join variables and equi-join predicates.

---

### 10. FK Triangle Row Count Coincidence Requires Explanation

The FK triangle (lineitem × partsupp × supplier) in `tpch-wcoj-selfjoin-sf001.csv` produces exactly 300,875 rows—the same as the self-join triangle in `tpch-wcoj-sf001.csv`. This coincidence of output sizes across structurally different queries is either:

(a) A genuine coincidence explained by the specific TPC-H SF=0.01 cardinalities, or
(b) Evidence that the two "FK triangle" experiments may be running the same query under different file names.

The paper should clarify whether these are indeed different queries, and if so, explain why they produce identical output cardinalities.

---

## Minor Issues

### 11. The "First in Open-Source" Claim Is Unverified

The abstract claims: *"Our approach is the first to combine WCOJ algorithms with multi-query optimization in an open-source, general-purpose SQL framework."* This is a strong claim that is not supported by a systematic review of open-source systems. The paper should either provide evidence (e.g., confirmed absence of WCOJ+MQO in DuckDB, Umbra, PostgreSQL extensions, etc.) or soften the claim to "to our knowledge" with a brief discussion.

---

### 12. Cyclicity Test Has an Unstated Assumption

Section 3.2 states that the cyclicity test "$|E| \geq |V|$" is "sound and sufficient." This is correct only for *connected* graphs—a disconnected graph with multiple components can satisfy $|E| \geq |V|$ while having no cycle. The paper implicitly assumes the join graph is always connected (which is true for any meaningful join query), but this assumption should be stated explicitly.

---

### 13. Memory Pressure of Spooling Is Not Discussed

Section 5.2 introduces `LogicalTableSpool` to materialize shared sub-expressions. For large batches at N=20 with 37,080 triangles per query variant, materializing 5 distinct sub-expression results requires significant intermediate memory. The paper mentions only that the system is tested with `-Xmx2g` (2 GB heap) but does not report peak memory usage or discuss what happens when materialization exceeds available memory. For the TPC-H self-join triangle with 2.9 million rows, a spool could require hundreds of MB.

---

### 14. `MULTI()` SQL Extension Is Non-Standard Without Standardization Path

The `MULTI()` SQL extension is a non-standard addition to Calcite's parser. While pragmatically useful, the paper does not discuss:
- How `MULTI()` interacts with standard SQL tooling (validators, planners, connectors built on Calcite).
- Whether this extension could be upstreamed to Apache Calcite or is purely a prototype.
- An alternative path using, e.g., standard `UNION ALL` rewrites or a planner hint mechanism.

For a systems paper targeting adoption, this is relevant scope.

---

## Data Verification Summary

All numbers in Tables 2, 3, and 5 were verified against the raw CSV files. All reported means match the raw data within rounding. The following discrepancies were found:

| Claim | Actual | Severity |
|-------|--------|----------|
| "All timings are mean over 10 iterations" | TPC-H 4-cycle uses only 5 iterations | **Methodological inconsistency** |
| FK triangle WCOJ speedup "1.1x" | Computed mean gives 1.05x | Minor rounding |
| FK rectangle/diamond not reported | WCOJ is 1.5x–3.5x *slower* than baseline | **Critical omission** |
| "Super-linear degradation of baseline" | 9x time for 16.7x edge growth (sub-linear) | **Incorrect claim** |
| Combine for TPC-H triangle is presented as improvement | Combine (3,884ms) > WCOJ (2,998ms) | **Underemphasized regression** |

---

## Recommendation

The paper addresses an interesting and well-motivated problem. The technical contributions (WCOJ integration, Combine operator, three-layer MQO) are substantive and the paper is generally clearly written. However, the experimental evaluation cannot be accepted in its current form due to:

1. Omission of benchmark results where WCOJ performs significantly worse than baseline
2. An inconsistent iteration count in the TPC-H experiments
3. Absence of statistical analysis despite high variance at N=2

These are not presentation issues—they affect the scientific validity of the claims. A thorough revision with complete benchmarks, statistical analysis, and honest discussion of failure modes is required. Once these issues are addressed, the paper could make a strong contribution to the database systems community.

---

## Suggested Additions for Revision

- **Table 6:** Complete TPC-H results including FK rectangle and diamond, with discussion of WCOJ regression conditions
- **Figure 1:** Log-log scaling plots for Table 2 to properly characterize growth rates
- **Error bars:** Standard deviations or 95% CI on all key bar/line charts
- **Section 7.3 (new):** "When Does WCOJ Help? Characterization of Failure Modes" analyzing the FK shapes and providing guidance for practitioners
- **Section 3.5 revision:** Extended cost model with search cost terms or explicit acknowledgment of model limitations
