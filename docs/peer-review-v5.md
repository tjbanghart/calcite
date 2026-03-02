# Peer Review: "Worst-Case Optimal Joins Meet Multi-Query Optimization: Shared Computation in Apache Calcite"

**Reviewer:** Anonymous PhD Reviewer
**Date:** 2026-02-25
**Recommendation:** Accept with Minor Revisions

---

## Summary

This paper presents an integrated system within Apache Calcite that combines worst-case optimal join (WCOJ) algorithms with multi-query optimization (MQO). The core contributions are the `Combine` relational operator and `MULTI()` SQL syntax for declarative query batching, a hash-based WCOJ implementation with multi-level trie indexing, and three cross-query optimizations: identity-based trie caching, frequency-aware sub-expression sharing via spools, and shared-prefix execution through join-variable fingerprinting. The paper proves correctness of prefix sharing, analyzes cost savings, and evaluates on both synthetic graph workloads and TPC-H-derived cyclic join queries.

The contribution is genuine and the paper is well-executed. The experimental evaluation is unusually honest: the authors characterize both the speedups (up to 4.0x synthetic, 3.1x TPC-H) and the failure modes (WCOJ 1.5x–3.6x *slower* on FK-heavy cyclic queries), which strengthens rather than weakens the paper's credibility. The statistical methodology is sound throughout. I recommend acceptance with two minor revisions described below.

---

## Strengths

**1. Novel and useful contribution.** No prior system combines WCOJ with MQO. The structural argument for why WCOJ creates qualitatively different sharing opportunities (trie sharing, search-space sharing, variable-level factoring) is convincing and clearly articulated in Section 2.5.

**2. Honest failure-mode reporting.** Table 6 reports FK rectangle (1.5x slower) and FK diamond (3.6x slower). The analysis in Section 7.2.4 correctly identifies the mechanism: WCOJ's per-variable enumeration cost dominates when the closing predicate is highly selective and binary joins handle it efficiently. This is the right scientific posture — characterizing the boundary of applicability rather than overselling.

**3. Sound statistical methodology.** All reported standard deviations use the sample formula (÷N−1, explicitly stated in the caption). High-variance cells are consistently marked with †, and speedup ratios derived from high-variance measurements are caveated with uncertainty ranges. These practices are more rigorous than typical systems papers.

**4. Clean ablation.** The combine-binary mode (`MULTI()` batching with binary joins, no WCOJ) directly decomposes the contributions of the join algorithm and the batching infrastructure. On the self-join triangle, combine-binary achieves 1.7x vs. WCOJ's 2.0x — showing batch amortization is a real contributor. On the FK triangle, combine-binary (599 ms) outperforms combine-WCOJ (741 ms) — confirming WCOJ actively hurts performance there. This is precisely the ablation needed to support the paper's claims.

**5. Real implementation.** The appendix lists 17 source files covering the full stack from SQL parser to enumerator, and the benchmark code is included in the repository with a CLI interface. The experiments are reproducible from stored artifacts.

---

## Issues

### Issue 1 (Minor): Theorem 1's Preconditions Are Only Met Under Combine-Share Mode

Theorem 1 states that prefix bindings are equal when variables have identical fingerprints. The proof sketch correctly notes that this requires the `TrieCache` to return *the same trie objects* for corresponding inputs, which in turn requires inputs to be the same Java object reference, which requires `CombineSharedComponentsRule` to be active (spool-based sharing). This means Theorem 1 does not hold in plain `Combine` mode — only in `Combine-Share` mode.

The paper documents this dependency in Section 5.1 and the proof sketch does condition on it. However, the theorem statement itself has no explicit precondition restricting it to Combine-Share mode. A reader who encounters the theorem in isolation will not know this.

**Required action:** Add a precondition to Theorem 1 making the Combine-Share mode dependency explicit, e.g.: *"Assume inputs are shared via `CombineSharedComponentsRule` so that corresponding trie objects are identical under `TrieCache` lookup."*

---

### Issue 2 (Minor): The Combine Regression Is Inferred, Not Measured

Section 7.2.4 attributes the 30% slowdown of plain Combine vs. sequential WCOJ on the self-join triangle to "the MULTI() batching overhead (plan compilation for the combined query, trie construction for all relations simultaneously)." This is plausible but inferred from the timing numbers rather than directly measured. The reader cannot verify whether plan compilation, trie construction, or some other factor (e.g., increased GC pressure from holding multiple concurrent data structures) is the dominant cause.

**Required action:** Either add a rough measurement separating compilation from execution cost, or soften the language to: "likely because of MULTI() batching overhead, though we have not isolated the precise contributing factor."

---

## Minor Observations (No Action Required)

**Table 6 omits Combine and Combine-Share columns.** The underlying CSV contains these modes for the FK rectangle and diamond, and both show similar regressions to WCOJ (1.5x and 3.6x slower, respectively). This is unsurprising and arguably not worth cluttering the table with, but a sentence noting that batching does not recover the regression would preempt the question.

**TPC-H workload representativeness.** The self-join queries are not standard TPC-H. The supply-chain co-occurrence motivation is reasonable. A production deployment would need to identify natural cyclic workloads; this paper establishes the building blocks.

**N=2 high variance.** At N=2, both batched modes have CV > 50%, making the reported means unreliable. The paper correctly attributes this to JIT effects and draws no conclusions from these cells.

---

## Data Verification

All means and sample standard deviations in Tables 2–6 were verified against the stored raw CSV files.

| Table | Claim | Status |
|:---:|:---|:---:|
| 2 | All means and std devs (synthetic scalability) | ✅ Match |
| 3 | All means and std devs (batch size scaling) | ✅ Match |
| 5 | All 15 verifiable cells (triangle, rectangle, FK-triangle × 5 modes) | ✅ Match |
| 5 | Rectangle combine-binary (OOM) | ✅ No CSV rows, consistent |
| 6 | FK rectangle and FK diamond (baseline + wcoj) | ✅ Match |
