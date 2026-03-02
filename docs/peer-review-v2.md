# Peer Review v2: "Worst-Case Optimal Joins Meet Multi-Query Optimization: Shared Computation in Apache Calcite"

**Reviewer:** Anonymous PhD Reviewer
**Date:** 2026-02-23
**Prior review:** `docs/peer-review.md`
**Recommendation:** Minor Revision Required

---

## Summary of Changes

The author addressed the majority of issues raised in the first review. The most critical problems—selective reporting of FK failure modes, missing iteration counts, unsupported claims about "super-linear" degradation, and absent statistical analysis—have all been substantively addressed. The paper is significantly stronger. However, the revision introduces two new critical errors and leaves several important issues unresolved, requiring another round of corrections before acceptance.

---

## Issues Resolved Since v1

The following items from the first review have been satisfactorily addressed:

| Issue | Status |
|-------|--------|
| "First in open-source" unqualified claim | ✅ Changed to "to our knowledge" |
| Selective reporting of FK failures | ✅ Table 6 added with FK rectangle (1.5x slower) and FK diamond (3.6x slower) |
| TPC-H 4-cycle only 5 iterations | ✅ Re-run with 10 iterations |
| No statistical analysis | ✅ Standard deviations and CV†-markers added |
| "Super-linear degradation" unsupported | ✅ Replaced with accurate relative comparison |
| Cyclicity test assumes connected graph | ✅ Assumption now explicitly stated |
| Missing batching ablation | ✅ Combine-binary column added to Table 5 |
| Combine regression vs. WCOJ unexplained | ✅ Explicitly discussed ("plain Combine 30% slower than WCOJ") |
| Cost model ignores search cost | ✅ "Limitations" paragraph added |
| TrieCache sharing scope unclear | ✅ Section 5.1 now distinguishes intra-operator vs. cross-operator sharing |
| Correctness proof relies on fragile identity | ✅ Proof sketch now conditions on Combine-Share mode activation |
| TPC-H query notation confusing | ✅ Rewritten in SQL |
| MULTI() standardization not discussed | ✅ Design alternatives paragraph added |
| Memory implications of spooling | ✅ Memory paragraph added to Section 5.2 |

---

## New Critical Issues (Introduced in Revision)

### 1. Combine-Binary Results Have No Backing Experimental Data

Table 5's `Combine-Binary` column is the most important new addition — it directly addresses the prior review's request for an ablation separating WCOJ from batching. However, **no CSV file in the repository contains combine-binary benchmark data**. Every stored benchmark file (`tpch-wcoj-selfjoin-sf001.csv`, `tpch-wcoj-sf001.csv`, and all synthetic files) contains exactly four modes: `baseline`, `wcoj`, `combine`, `combine-share`. The combine-binary mode exists in `TpchWCOJBenchmarkCli.java` but was never run to completion and saved.

The reported values (3,378 ± 93 ms for triangle, OOM for 4-cycle, 519 ± 25 ms for FK triangle) are either manually transcribed from transient console output during development, or the CSV was not saved. In either case, these numbers are currently **unverifiable and unreproducible from the stored artifacts**.

This is particularly damaging because the OOM claim for Combine-Binary on the 4-cycle is the most striking result in the revised paper — it reframes the 4-cycle result from a speedup to a qualitative feasibility difference. An unverified OOM claim presented as experimental fact is a serious integrity issue.

**Required action:** Re-run the TPC-H benchmark with the combine-binary mode and commit the resulting CSV to the repository. The OOM claim should include the JVM heap size at which OOM occurs (e.g., "fails with -Xmx4g"), the exception class, and ideally a heap utilization plot or log snippet.

---

### 2. Standard Deviations in Table 5 Are Computed Incorrectly and Inconsistently

Verification against the raw CSV reveals that standard deviations in Table 5 are wrong for multiple cells:

| Cell | Paper Reports | Population Std (÷N) | Sample Std (÷N-1) | Match |
|------|--------------|---------------------|--------------------|-------|
| Self-join △ baseline | ±1,162 | 1,280 | 1,349 | **Neither** |
| Self-join △ WCOJ | ±376 | 403 | 425 | **Neither** |
| Self-join □ baseline | ±28,971 | **28,971** ✓ | 30,538 | Population |
| FK △ baseline | ±107 | 103 | 109 | **Neither** |
| FK △ WCOJ | ±243 | **241** ≈ | 254 | ≈ Population |

The rectangle baseline matches the population standard deviation (dividing by $N$ rather than $N-1$) exactly. The FK-triangle values approximately match population std. The self-join triangle values match neither formula. This inconsistency suggests that different cells were computed using different methods — possibly a mix of spreadsheet formulas, manual calculations, or different subsets of the data.

Using population std instead of sample std is a statistical error: with only 10 observations, the sample standard deviation (dividing by $N-1 = 9$) is the correct unbiased estimator. For the rectangle baseline, the difference is 28,971 vs. 30,538 — a 5% underestimate. More importantly, for cells where neither formula matches (triangle baseline: paper says 1,162, actual sample std is 1,349), the standard deviation appears to be simply wrong, with no recoverable explanation.

**Required action:** Recompute all standard deviations from the raw CSV data using the sample standard deviation formula ($s = \sqrt{\frac{1}{N-1}\sum(x_i - \bar{x})^2}$). If a reporting script was used, audit it for the population vs. sample distinction. Regenerate Table 5 with corrected values.

---

## Important Issues Remaining from v1

### 3. Triangle Baseline in Table 5 Missing the "†" High-Variance Marker

By the paper's own convention (stated in Section 7.2 and Table 3), cells with coefficient of variation (CV) > 20% receive a "†" marker. Computing from the raw data:

- Self-join triangle baseline: mean = 6,102ms, sample std = 1,349ms, **CV = 22.1% → should be marked †**

The triangle baseline in Table 5 is not marked with †, while the 4-cycle baseline (CV = 23.8%) correctly receives the marker. The omission is inconsistent and relevant: the speedup ratios in Table 5 (2.0x for WCOJ, 1.6x for Combine, 2.4x for Combine-Share) are all computed against this high-variance baseline. If the denominator is noisy, so are all derived speedup claims.

**Required action:** Apply the † marker to the self-join triangle baseline in Table 5. Acknowledge in the analysis text that all triangle speedup ratios are computed against a high-variance baseline.

---

### 4. FK-Triangle WCOJ Speedup (1.1x) Is Not Statistically Meaningful

The FK-triangle WCOJ row has CV = 28.9% (correctly marked with †, with individual values ranging from 668ms to 1,466ms — over a 2x range). Despite this, the paper reports "WCOJ (1.1x)" as a positive speedup. With such variance, the means of baseline (927ms) and WCOJ (882ms) are not statistically distinguishable. In some individual iterations, WCOJ is slower than baseline.

The statement in Section 7.2.4 that "WCOJ alone barely improves on baseline (1.1x)" implies WCOJ still helps, even modestly. A more accurate statement is: **WCOJ shows no statistically distinguishable improvement over binary joins on the FK triangle at this scale**. This is itself a meaningful finding — it correctly locates the boundary of WCOJ's utility — but it should be stated accurately.

**Required action:** Restate the FK-triangle WCOJ result as "not significantly different from baseline" or add a confidence interval that makes the overlap explicit. Do not present a 1.1x mean as evidence of improvement when the measurements have CV > 28%.

---

### 5. Self-Join 4-Cycle Query Description Has a Repeated Join Column

Section 7.2.4 describes the self-join 4-cycle as: *"four copies of lineitem joined on suppkey, orderkey, partkey, orderkey."* The column `orderkey` appears twice. A proper 4-cycle requires four distinct join predicates to form a cycle. The description implies the query has the structure:

```
L1.suppkey = L2.suppkey
L2.orderkey = L3.orderkey
L3.partkey = L4.partkey
L4.orderkey = L1.orderkey   ← shares column name with join at level 2
```

If `L2.orderkey = L3.orderkey` and `L4.orderkey = L1.orderkey` are two separate join predicates over the same column attribute, this is a valid 4-cycle (the cycle closes through `orderkey`). But the description is ambiguous and should be clarified. Either present the full SQL (as was done for the triangle) or use a Datalog notation with distinct variable names:

$$Q_\square(l_1, l_2, l_3, l_4) \leftarrow L_1(\_, s, o_1, \_), L_2(\_, s, \_, p), L_3(\_, \_, o_2, p), L_4(\_, \_, o_1, \_), L_4.o = L_1.o$$

**Required action:** Add the full SQL for the 4-cycle query, mirroring the triangle's SQL block, or use an unambiguous formal notation that names all join variables explicitly.

---

### 6. High-Variance Means Used as Speedup Denominators Without Caveat

Beyond the FK-triangle case, there are additional places where speedup ratios are derived from high-variance means:

- **Table 3, N=10 Baseline**: 559 ± 176ms (CV = 31.5%, marked †). The headline speedups 3.0x (Combine) and 3.4x (Combine-Share) both use this value as denominator. At the lower end of the CI, baseline could be ~400ms, making these speedups closer to 2.1x and 2.4x.

- **Table 3, N=5 WCOJ**: 182 ± 44ms (CV = 24.2%, marked †). The reported speedup "1.5x" is not caveated, despite deriving from a high-variance numerator.

The paper correctly marks high-variance cells with † but then proceeds to compute and report speedup ratios from them without qualification. Adding the markers without adjusting the discussion treats the symptom without the cause.

**Required action:** When reporting a speedup whose numerator or denominator has CV > 20%, either: (a) widen the stated speedup to a range (e.g., "1.5x–3.1x depending on JIT state"), or (b) explicitly note that the ratio is an estimate with high uncertainty in the discussion text.

---

## Minor Issues

### 7. JIT Attribution of N=2 Variance Is Not Applicable to TPC-H Long Queries

Section 7.2.2 attributes the N=2 high variance to: *"JIT compilation effects: the first measured iteration often triggers compilation of the MULTI() code path."* This explanation is plausible for the 30–400ms synthetic queries. However, the same explanation is then implicitly extended to the TPC-H triangle (where individual runs take 2–8 seconds) and the 4-cycle (where first-iteration baseline takes 212 seconds while subsequent iterations stabilize at 110–130 seconds). For queries taking 2+ minutes, JVM JIT compilation (which completes in milliseconds) cannot explain 80-second outliers. The 4-cycle first-iteration outlier more likely reflects OS page cache effects, JVM GC pressure, or thermal throttling on the M4 Pro under sustained load.

**Required action:** Separate the JIT explanation (appropriate for short synthetic queries) from the explanation for TPC-H variance. For the 4-cycle 212-second outlier specifically, investigate and state the likely cause (OS page cache cold start, GC pause, etc.).

---

### 8. TPC-H Benchmark Uses a Different Warmup Count Without Justification

Section 7.1 now states: *"The synthetic graph workload uses 3 warmup rounds; the TPC-H workload uses 5."* No justification is given for why TPC-H needs more warmup rounds, and the CSV data cannot confirm warmup iteration counts (only measured iterations are stored). If the TPC-H warmup change was applied only to the re-run 4-cycle data (to address the reviewer's concern about 5-iteration inconsistency), it should be stated that this was a methodological change made in the revision. If it applies to all TPC-H experiments, it may affect comparability with the original triangle and FK-triangle runs that predate this change.

**Required action:** State when the warmup count change was applied (all TPC-H, or only the re-run 4-cycle) and verify that original TPC-H triangle and FK-triangle results are not affected.

---

### 9. N=2 Speedup for WCOJ Reported Without Context

Table 3, N=2 row shows WCOJ at 119 ± 32† (CV = 26.9%) with "(1.9x)" speedup, while the Combine and Combine-Share rows omit their speedup parentheticals. This inconsistency makes WCOJ look favorable at N=2 by selectively presenting its speedup despite the same reliability concerns that warranted omitting the batched-mode speedups. Either report all speedups at N=2 with † caveats, or omit all of them.

---

## Summary Assessment

**What the revision got right:** The author responded in good faith to the substantive criticisms. Adding Table 6 with negative results, re-running the 4-cycle, clarifying TrieCache scope, adding cost model limitations, and discussing the Combine regression vs. WCOJ are all meaningful improvements. The paper is now more honest about WCOJ's limitations than most systems papers.

**What still needs fixing:** The revision introduced two new critical errors — unverified combine-binary data and incorrect standard deviations — that undermine the statistical credibility of the very additions meant to address reviewer concerns. These are mechanical errors that should be straightforward to correct.

**Path to acceptance:** The paper requires (1) re-running and archiving the combine-binary benchmark, (2) recomputing all standard deviations from raw data, and (3) adding the † marker to the omitted triangle baseline cell. Items 3–9 above are important but can be addressed editorially without new experiments. The core contributions remain sound and the results (even when correctly analyzed) support the paper's claims for the cases where WCOJ applies.

---

## Verification Summary

All means in Tables 2, 3, and 5 were re-verified against current raw CSV files. All means match to within rounding. Standard deviation discrepancies are documented in Issue 2 above.

| Data claim | Verified? |
|-----------|-----------|
| All means in Tables 2, 3, 5 | ✅ Match raw CSV |
| Standard deviations in Table 5 | ❌ Multiple cells wrong (see Issue 2) |
| Combine-binary results in Table 5 | ❌ No backing CSV data exists |
| Table 6 FK rectangle/diamond | ✅ Match `tpch-wcoj-sf001.csv` |
| TPC-H 4-cycle now has 10 iterations | ✅ Confirmed in updated CSV |
| Synthetic benchmarks (Tables 2–4) | ✅ Unchanged, previously verified |
