# Paper Review v5: Length Reduction

**Date:** 2026-03-02
**Goal:** Cut from ~35 pages. Recommendations ordered by estimated savings.

---

## High-Impact Cuts

### 1. Section 7.2.2 (Multi-Query Speedup) — cut ~1.5 pages
The N=2 row is noise (CV > 50%) and the analysis paragraph explaining it is longer than the insight warrants. The N=20 reversal analysis repeats what 7.2.3 covers in the ablation. Recommend:
- Drop N=2 row from Table 3
- Cut the JIT compilation variance paragraph (lines 538-539) to one sentence
- Merge the N=20 reversal observation into 7.2.3 rather than explaining it twice

### 2. Section 2.1 + 2.2 (WCOJ Theory + Implementations) — cut ~1 page
The Skew Strikes Back / Minesweeper / Gottlob et al. / Ngo tutorial paragraph (line 67) is thorough but most of it isn't referenced again. The paper uses Generic-Join and the AGM bound — that's it. Recommend:
- Cut the Minesweeper, BRR bound, and hypertree width material to a single sentence acknowledging extensions exist
- Keep AGM bound, Generic-Join, and the fractional edge cover LP — those are load-bearing

### 3. Section 4.2 (MULTI Design Alternatives) — cut ~0.5 page
The three-alternative comparison (UNION ALL, hints, MULTI) is longer than needed. The UNION ALL and hint dismissals can each be one sentence. The "upstreaming to Calcite" paragraph and "application-level middleware" sentence are speculative and not part of the contribution.

### 4. Section 7.2.4 Analysis Paragraphs — cut ~1 page
The analysis after Tables 5-6 is the longest prose block in the paper. Several points are made twice:
- "WCOJ benefits depend on join structure" restates what the tables already show
- The variance caveat paragraph repeats the methodology note from 7.2's opening
- The "Combine regression" paragraph and the "ablation: batching vs WCOJ" paragraph overlap — both explain when Combine helps/hurts
Recommend consolidating into two focused paragraphs: (1) when WCOJ wins and why, (2) when it loses and why.

### 5. Section 2.3 (MQO Background) — cut ~0.5 page
The Sellis/Finkelstein historical setup is standard and this audience knows it. The Kathuria/Sudarshan NP-hardness result matters (you reference it to say you sidestep the selection problem), but Jindal, Zinchenko, and the industry paragraph (Bruno, Roy/SparkCruise) each get 2-3 sentences that could be one. The QPipe comparison could move to a parenthetical.

---

## Medium-Impact Cuts

### 6. Section 3.2 (Cyclic Query Detection) — trim ~0.5 page
v3 already flagged this. The three-step pipeline is clear but step-by-step narration ("Step 1... Step 2... Step 3...") is verbose for what amounts to: flatten joins, build hypergraph, check |E| >= |V|. The DpHyp paragraph is good but the "separate constructions" implementation detail at the end can go.

### 7. Section 5.1 (Trie Caching) — trim ~0.25 page
The Java code block for TrieCache.getOrBuild is helpful, but the three bullet points after it (identity-based lookup, lazy construction, lifecycle) restate what the code already shows. Keep one sentence summarizing the design rationale (identity-based, not structural equality) and cut the rest.

### 8. Section 6.1 (Correctness Proof) — trim ~0.25 page
The proof sketch walks through the argument carefully, which is good, but the parenthetical about conditions (1) and (2) for TrieCache identity repeats material from 5.1 and 5.2 verbatim. A forward reference suffices.

---

## Low-Impact / Leave Alone

- **Section 1 (Intro)**: Already tight. The four contributions are load-bearing.
- **Section 3.0 (System Overview)**: The diagram is the best part of the paper. Don't touch it.
- **Section 3.3 (Hash Tries)**: Concise, good example.
- **Section 3.4 (WCOJ Enumerator)**: The pseudocode earns its space.
- **Section 5.3 (Prefix Sharing)**: Dense but every part is referenced by the proof or experiments.
- **Section 7.2.1 (Graph Size Scalability)**: Compact, Table 2 does the work.
- **Section 8 (Conclusion)**: Already appropriate length.
- **Appendix A**: Useful reference, minimal space.

---

## Estimated Savings

| Cut | Est. pages saved |
|:----|:---:|
| 7.2.2 analysis consolidation | 1.5 |
| 2.1-2.2 theory trimming | 1.0 |
| 7.2.4 analysis consolidation | 1.0 |
| 4.2 design alternatives | 0.5 |
| 2.3 MQO background | 0.5 |
| 3.2 cyclic detection | 0.5 |
| 5.1 trie caching bullets | 0.25 |
| 6.1 proof deduplication | 0.25 |
| **Total** | **~5.5** |

That would bring the paper from ~35 to ~29-30 pages.
