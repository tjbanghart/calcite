# Paper Review v6: Em-Dash Elimination

**Date:** 2026-03-02
**Goal:** Remove all em-dashes. Replace with periods, commas, semicolons, or parentheses as appropriate.

27 occurrences found. Grouped by replacement strategy.

---

## Split into two sentences (12 instances)

These em-dashes introduce a clause that works better as its own sentence.

| Line | Current | Suggested |
|:---:|:---|:---|
| 33 | "...the failure modes — FK-based cycles where..." | "...the failure modes. FK-based cycles where..." |
| 73 | "...requires pre-sorted tries — a constraint that limits..." | "...requires pre-sorted tries. This constraint limits..." |
| 75 | "...does not support general SQL workloads — both gaps our system addresses." | "...does not support general SQL workloads. Our system addresses both gaps." |
| 181 | "...the join ordering question is sidestepped — WCOJ processes all variables..." | "...the join ordering question is sidestepped. WCOJ processes all variables..." |
| 265 | "...routing each sub-query to the strategy best suited to its join structure — a design goal confirmed by..." | "...routing each sub-query to the strategy best suited to its join structure. The experimental results in Section 7.2.4 confirm this design goal." |
| 358 | "...the *same Java object reference* — structural equality would require..." | "...the *same Java object reference*. Structural equality would require..." |
| 474 | "...grows as $O(\|Q(D)\|)$ — proportional to the output size, since..." | "...grows as $O(\|Q(D)\|)$, proportional to the output size. Prefix bindings must be materialized..." |
| 538 | "...regains the lead (2.7x vs. 2.4x) — a reversal explained by..." | "...regains the lead (2.7x vs. 2.4x). Section 7.2.3 explains this reversal." |
| 557 | "...When $C_{\text{coord}}$ dominates — as at $N=20$..." | "...When $C_{\text{coord}}$ dominates, plain Combine is preferable. At $N=20$ in Table 3, the prefix-sharing coordination overhead grows with output size while plain Combine benefits from trie caching alone." |
| 588 | "...`MergeJoinEnumerator.toLookup_` — the 4-way binary join intermediate exceeds heap." | "...`MergeJoinEnumerator.toLookup_`; the 4-way binary join intermediate exceeds heap." |
| 630 | "...Future work item (4) — hybrid join strategies within a batch — directly addresses this." | "...Future work item (4), hybrid join strategies within a batch, directly addresses this." |
| 634 | "...(4) hybrid join strategies within a batch — using WCOJ for cyclic components..." | "...(4) hybrid join strategies within a batch, using WCOJ for cyclic components..." |

## Replace with parentheses (5 instances)

These are genuine asides or parenthetical clarifications.

| Line | Current | Suggested |
|:---:|:---|:---|
| 255 | "When it is small...the lower bound is a useful approximation. When $C_{\text{search}}$ dominates — as in FK-based cycles where..." | "When $C_{\text{search}}$ dominates (as in FK-based cycles where a variable has high fan-out...)" |
| 257 | "...identifying the structural conditions — high fan-out at intermediate variables with low final selectivity — that cause the gap." | "...identifying the structural conditions (high fan-out at intermediate variables with low final selectivity) that cause the gap." |
| 287 | "...which rules out batches where queries project different columns — the common case when exploring..." | "...which rules out batches where queries project different columns (the common case when exploring different facets of the same join structure)." |
| 561 | "...introduce a **combine-binary** mode — `MULTI()` batching with standard binary hash joins, no WCOJ —" | "...introduce a **combine-binary** mode (`MULTI()` batching with standard binary hash joins, no WCOJ)" |
| 586 | "...each linking a different pair of lineitem copies — this is valid since the cycle traverses..." | "...each linking a different pair of lineitem copies (valid because the cycle traverses four copies through four distinct edges)." |

## Replace with comma or semicolon (5 instances)

These connect closely related clauses where a lighter separator works.

| Line | Current | Suggested |
|:---:|:---|:---|
| 35 | "...providing a platform for *selective* optimization — routing cyclic sub-queries to WCOJ..." | "...providing a platform for *selective* optimization, routing cyclic sub-queries to WCOJ..." |
| 77 | "...the closest precursor to ours — we adopt the same hash-trie approach — but their system..." | "...the closest precursor to ours; we adopt the same hash-trie approach, but their system..." |
| 179 | "...becomes a `JoinVariable` — a hyperedge in the join hypergraph." | "...becomes a `JoinVariable`, a hyperedge in the join hypergraph." |
| 251 | "...captures the two unavoidable costs — reading each input once and producing each output tuple — but omits..." | "...captures the two unavoidable costs (reading each input once and producing each output tuple) but omits..." (parentheses better here actually) |
| 574 | "...linked by shared orders, suppliers, and parts — a supply-chain co-occurrence pattern." | "...linked by shared orders, suppliers, and parts, a supply-chain co-occurrence pattern." |

## Table cell — leave as-is (1 instance)

| Line | Current | Reason |
|:---:|:---|:---|
| 548 | `— ` in "Marginal gain" column | Standard table notation for "not applicable". Keep. |

## Already addressed in other edits (4 instances)

Lines 446, 255 (second clause), 607, 609, 611 contain em-dashes in the longer omitted passages. These all follow the same patterns above. Apply the same rules: aside → parentheses, independent clause → new sentence.

---

## Summary

- 12 → new sentence
- 5 → parentheses
- 5 → comma/semicolon
- 1 → keep (table notation)
- 4 → covered by above patterns
