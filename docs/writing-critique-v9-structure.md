# Writing Critique v9: Structural and Narrative Issues

**Date:** 2026-03-15
**Scope:** Third-pass critique of `wcoj-mqo-calcite-v2.md`. Sentence-level issues from v7/v8 are mostly resolved. This pass focuses on narrative arc, structural gaps, and things a reviewer would flag.

---

## Overall

The prose is now clean. The remaining issues are about how the paper is *organized* and what it *doesn't say*, not how individual sentences read.

---

## 1. The Story is Told Backwards

The paper's most interesting and honest finding is the failure mode analysis: WCOJ is 1.5x-3.6x *slower* on FK cycles. This is what makes the paper credible and useful to practitioners. But the paper leads with the wins (4x! 3.1x! 9.5x amortization!) and treats the losses as a late-arriving caveat in Section 7.2.4.

The conclusion gets this right: "The value of `Combine` is that it is agnostic to join strategy." But the intro doesn't set this up. The intro's framing is "WCOJ is great, MQO is great, we combine them." A stronger framing would be: "WCOJ helps on some cyclic queries and hurts on others. We built a framework that lets the optimizer choose."

This doesn't require a rewrite, just adjusting emphasis. In the intro, after listing the three cross-query optimizations, add a sentence like: "We show that WCOJ is not always beneficial, and characterize the conditions that determine when it helps."

---

## 2. Section 2.5 and Section 5 Say the Same Thing Twice

Section 2.5 ("The Unexplored Intersection") lists three sharing opportunities:
1. Trie sharing
2. Search-space sharing
3. Variable-level factoring

Section 5's opening paragraph lists three optimizations targeting "different levels of redundancy":
1. Trie caching shares *data structures*
2. Sub-expression sharing shares *sub-plans*
3. Prefix sharing shares *computation*

These are the same three things described with different terminology. The reader encounters the same taxonomy twice. Either cut Section 2.5 down to a single sentence ("WCOJ's structure creates sharing opportunities unavailable to binary-join planners; we describe these in Section 5") or make the two sections explicitly the same list with the same names.

---

## 3. Variable Ordering is Buried

Variable ordering is one of the most important decisions in any WCOJ implementation. It affects all WCOJ performance, not just prefix sharing. But the only mention is in Section 5.3.2:

> "The current implementation uses the order in which equivalence classes are discovered during join graph analysis (Section 3.2), which follows the syntactic order of join predicates."

This should be in Section 3 (WCOJ integration), not Section 5 (prefix sharing). A reviewer will ask: "What variable ordering do you use? How sensitive are the results to it?" Right now the answer is buried in a subsection about a different feature.

Move this to Section 3.4 (enumerator) or create a short Section 3.6 on variable ordering. Acknowledge that the ordering is not optimized and that this is a limitation.

---

## 4. combine-binary Appears Without Introduction

Table 5 introduces a "C-Bin" column (combine-binary mode: MULTI() with binary joins, no WCOJ). This mode wasn't mentioned in the experimental setup (Section 7.1), which describes only four modes: baseline, wcoj, combine, combine-share.

Either add combine-binary to the mode table in Section 7.1, or introduce it in the TPC-H subsection before showing the table. Right now it's a surprise column.

---

## 5. The 2x2 Matrix is Implicit

The paper has results for {binary, wcoj} x {sequential, batched}, but never explicitly frames it this way. The four modes form a natural 2x2:

|              | Sequential | Batched (Combine) |
|:-------------|:----------:|:-----------------:|
| Binary joins | baseline   | combine-binary    |
| WCOJ         | wcoj       | combine           |

This framing would make the contribution decomposition much clearer. Readers could see at a glance: "How much comes from WCOJ vs. binary? How much comes from batching vs. sequential?" The combine-binary column in Table 5 already provides this decomposition, but the paper doesn't draw it out.

---

## 6. Section 3.2 Still Has a Dense Paragraph

The GYO example is great. But the paragraph above it is still a wall:

> "Calcite represents joins as a binary tree of `LogicalJoin` nodes, where each node carries only its local condition. Cyclicity is not detectable at any single node. `JoinToMultiJoinRule` (a standard Calcite rule) collapses the inner-join subtree into a single `MultiJoin` with $N$ inputs and one combined equi-join condition, making the global structure accessible. From this combined condition, we extract equi-join predicates and group co-equated fields into equivalence classes using Union-Find. Each equivalence class spanning two or more inputs becomes a `JoinVariable`, a hyperedge in the join hypergraph. The implementation handles general hyperedges of any degree; all queries in our evaluation happen to be degree-2."

This is 6 sentences doing 3 things: (a) explaining why flattening is needed, (b) describing how Calcite flattens joins, (c) describing how equivalence classes become hyperedges. Break it into shorter paragraphs or trim (a), since your reader can infer why flattening is needed from the explanation of how.

---

## 7. Cost Model (Section 6.2) Works Through a Specific Case That Doesn't Add Much

> "For $N$ identical triangle queries differing only in projection, $K = 3 = m$ (all variables shared), so $\bar{S} \approx 0$ and the savings approach $(N-1) \cdot C_{\text{full\_WCOJ}} - C_{\text{coord}}$. The coordination overhead $C_{\text{coord}}$ grows as $O(|Q(D)|)$, proportional to the output size. Prefix bindings must be materialized and dispatched to suffix executors, so the net benefit depends on $N$ being large enough for the $(N-1) \cdot P$ savings to dominate."

The general formula already says this. Working through K=3=m is hand-holding. Either cut this paragraph or move it to the experimental analysis where you can point at actual numbers.

---

## 8. C_coord is Defined but Never Decomposed

Section 6.2 lists four components of C_coord: TrieCache lookup, spool materialization, prefix binding materialization, and per-binding dispatch. But neither the cost analysis nor the experiments say which component dominates. When Section 7.2.4 says "coordination overhead on 6M rows dominates," the reader doesn't know if that's spool materialization, prefix dispatch, or something else.

If you can't measure the components separately, at least state which one you *believe* dominates and why (likely the `.toList()` materialization, based on the Combine overhead discussion).

---

## 9. Unreferenced or Barely-Referenced Citations

29 references for a ~5000-word paper is heavy. Spot-checking:

- [12] Gurumurthy et al. -- not cited in the text at all (unless I missed it)
- [13] Tian -- cited once, to note a "trend" that your work addresses
- [16] Michiardi -- not cited in the text
- [17] Schonberger -- not cited in the text
- [18] Ngo et al. (Skew strikes back) -- not cited in the text
- [24] Ngo (open problems survey) -- not cited in the text
- [28] Moerkotte and Neumann (DpHyp) -- not cited in the text

If these were removed from the previous revision's prose but left in the reference list, clean them up. Dangling references look sloppy and a reviewer will notice.

---

## 10. The Paper Doesn't Discuss When to Use WCOJ vs. Binary

This is the practical question a Calcite user would ask: "Should I enable this for my workload?" The paper shows WCOJ wins on self-join cycles and loses on FK cycles, but doesn't distill this into guidance. Section 7.2.4 explains *why* (selective closing predicates), but the conclusion just says "WCOJ is not universally beneficial."

Consider adding 2-3 sentences of practical guidance, either at the end of Section 7.2.4 or in the conclusion: "WCOJ is most beneficial when (a) the query is cyclic, (b) intermediate results would be large relative to the output, and (c) the cycle-closing predicate is not highly selective. When these conditions are not met, binary joins should be preferred."

---

## Summary

| Issue | Where | Fix |
|-------|-------|-----|
| Story led by wins, not insight | Intro | Add a sentence framing failure modes as a contribution |
| Section 2.5 / Section 5 duplication | 2.5, 5 | Unify terminology or make 2.5 a forward pointer |
| Variable ordering buried | 5.3.2 | Move to Section 3 |
| combine-binary introduced without setup | 7.2.4 | Add to mode table in 7.1 or introduce before Table 5 |
| 2x2 decomposition implicit | 7 | Add a framing sentence or small table |
| Section 3.2 dense paragraph | 3.2 | Break into two shorter paragraphs |
| Cost model works through obvious case | 6.2 | Cut the K=3 worked example |
| C_coord never decomposed | 6.2, 7.2.4 | State which component dominates |
| Dangling references | References | Remove [12], [13], [16], [17], [18], [24], [28] if uncited |
| No practical guidance | 7.2.4 or 8 | Add 2-3 sentences on when to use WCOJ |

The paper is close. These are the kinds of issues a reviewer sends back as "minor revisions."
