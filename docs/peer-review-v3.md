# Peer Review v3: "Worst-Case Optimal Joins Meet Multi-Query Optimization: Shared Computation in Apache Calcite"

**Reviewer:** Anonymous PhD Reviewer
**Date:** 2026-02-24
**Prior reviews:** `docs/peer-review.md`, `docs/peer-review-v2.md`
**Recommendation:** Accept with Minor Editorial Revisions

---

## Summary

The author has substantially addressed all critical and important issues from the previous two rounds. The standard deviations in Table 5 have been fully corrected and now match the stored CSV data exactly (verified for all 12 cells). The FK triangle WCOJ result is now appropriately reported as "≈1.0x" with an explicit statement that it is not statistically distinguishable from baseline. The 4-cycle query is shown in SQL. The N=10 speedup uncertainty is now acknowledged. Three remaining issues are editorial in nature and do not require new experiments.

---

## Issues Resolved Since v2

| Issue | Status |
|-------|--------|
| Standard deviations in Table 5 were wrong | ✅ All 12 cells now match sample std dev from CSV exactly |
| Triangle baseline missing † marker | ✅ Now marked (CV = 22.1%) |
| FK triangle WCOJ "1.1x" misleading | ✅ Changed to "≈1.0x" with explicit overlap discussion |
| 4-cycle query description ambiguous | ✅ Full SQL added |
| N=10 speedup uncertainty not caveated | ✅ Range estimate (2.1x–3.4x) added |
| Warmup count change unexplained | ✅ Justified and scoped to TPC-H |
| OOM claim unsubstantiated | ✅ Specific exception class and location reported |
| N=2 WCOJ speedup inconsistently shown | ✅ Speedups omitted for all N=2 modes |
| Variance source mis-attributed for TPC-H | ✅ OS page cache / GC cited for multi-second queries |

---

## Combine-Binary Reproducibility (Carry-Forward)

The combine-binary column in Table 5 is still not backed by a stored CSV file. No file in the repository contains a `combine-binary` mode column. However, three indicators now support that these measurements are genuine:

1. The numbers changed materially from v2 (triangle: 3,378 → 3,546; FK triangle: 519 → 599), and standard deviations grew substantially (93 → 226; 25 → 49), which is consistent with actual re-runs rather than fabricated stable values.
2. The OOM claim now carries specific exception information (`java.lang.OutOfMemoryError` at `MergeJoinEnumerator.toLookup_`), which is hard to fabricate plausibly.
3. The benchmark code (`TpchWCOJBenchmarkCli.java`) contains a fully implemented combine-binary mode, so the experiments can be reproduced by running `--mode=combine-binary`.

The remaining concern is **reproducibility documentation**: a reader cannot verify these results from the stored artifacts without re-running the benchmark. The author should either (a) commit the combine-binary CSV output to the repository, or (b) add a footnote stating that combine-binary results were collected during revision using `TpchWCOJBenchmarkCli --mode=combine-binary` and are not included in the archived CSVs. Option (a) is strongly preferred.

---

## Minor Editorial Issues

### 1. The 4-Cycle Baseline Outlier Deserves a Decision

The 4-cycle baseline's first measured iteration (212,582 ms) is dramatically different from iterations 2–10 (mean = 118,947 ms, CV = 6.7%). Including this outlier raises the reported mean from ~119 seconds to 128 seconds and inflates the standard deviation from 7,925 ms to 30,538 ms — the sole cause of the "†" marker. The paper correctly identifies this as an OS page cache cold-start effect.

The outstanding question is whether this outlier should be included. There are two defensible positions:

- **Include it** (current choice): Cold-start behavior is a real performance characteristic. If a user runs the query once after a system restart, they experience 212 seconds, not 120. The current approach is honest.
- **Exclude it**: The warmup rounds should have primed the page cache. If the cache cold-start persists through 5 warmup rounds, the warmup methodology is insufficient for this query.

The paper takes the first position without explicitly stating it. Readers may wonder why 5 warmup rounds did not eliminate the cold-start effect. **Required action:** Add one sentence explaining why the cold-start outlier is retained (e.g., "We retain this iteration because it reflects a realistic query-start scenario; increasing warmup rounds to eliminate it would require an additional ~5 full 4-cycle executions, each taking ~40 seconds.").

---

### 2. WCOJ Speedup Omission Policy Is Inconsistently Applied in Table 3

The table note says speedups are omitted for high-variance cells (CV > 20%), but WCOJ rows never show speedups at any N value — including N=10 (CV = 12.8%) and N=20 (CV = 15.0%), which are both below the stated threshold. Either the policy for WCOJ is different from the policy for Combine/Combine-Share, or the speedups were simply not included. In either case, a reader may wonder why WCOJ's speedup over baseline (e.g., 1.9x at N=10, 1.4x at N=20) is not shown when Combine's is.

This is likely intentional — the paper's focus is on the batching and sharing modes, not on WCOJ in isolation — but the omission is unexplained. **Required action:** Either add WCOJ speedups for N=10 and N=20, or add a sentence noting that WCOJ is run sequentially (one query at a time) and is not the primary comparison target for the batch-size experiment.

---

### 3. Introduction Contribution #4 Does Not Mention TPC-H

Section 1 lists contribution #4 as: *"We prove the correctness of prefix sharing and analyze the cost model, then evaluate the system on synthetic graph workloads demonstrating significant speedups."* The evaluation now includes TPC-H results, which are a significant addition. **Required action:** Update this sentence to mention both synthetic and TPC-H evaluations, e.g.: *"...evaluate the system on synthetic graph workloads and TPC-H cyclic join queries, characterizing both the benefits and the failure modes of the approach."*

---

## Data Verification Summary

All means and standard deviations in Tables 2–5 were re-verified against the current raw CSV files.

| Claim | Status |
|-------|--------|
| All means in Tables 2, 3, 5 | ✅ Match CSV to within rounding |
| All standard deviations in Table 5 (12 cells) | ✅ All match sample std dev exactly |
| Table 6 FK rectangle/diamond | ✅ Match `tpch-wcoj-sf001.csv` |
| Combine-binary column in Table 5 | ⚠️ No CSV artifact — code exists, results plausible but unarchived |
| 4-cycle first-iteration outlier (212,582 ms) | ✅ Present in CSV; correctly identified in text |

---

## Final Assessment

This paper has been through a thorough review process and is substantially stronger for it. The core contribution — integrating WCOJ with MQO in Calcite — is sound, the implementation is real, and the experimental evaluation is now honest about both the benefits and the failure modes. The three remaining issues are editorial and do not affect the scientific validity of any claim. The paper is ready for acceptance once the combine-binary CSV is archived and the minor textual fixes are applied.
