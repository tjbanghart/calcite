# Writing Critique v3: "Worst-Case Optimal Joins Meet Multi-Query Optimization"

**Date:** 2026-03-02
**Scope:** Follow-up to `docs/writing-critique-v2.md`. Assesses revisions to implementation detail sections (primarily Sections 3.2, 3.4, 5.2) and the addition of DpHyp context.

---

## Overall Assessment

**Grade: A.** The paper remains camera-ready. The revisions add genuine technical depth to the implementation sections — the cyclic query detection pipeline, the WCOJ enumerator's backtracking correctness argument, and the memory considerations for spooling all strengthen the systems contribution. The DpHyp relationship paragraph is the most architecturally interesting addition. The remaining issues are about calibrating the level of implementation detail for a paper audience versus a codebase audience.

---

## Changes Since v2

| Section | Change | Assessment |
|:---|:---|:---:|
| 3.2 (Cyclic Query Detection) | Expanded from brief cyclicity test to three-step pipeline with hypergraph construction details | See Issue 1 |
| 3.2 (new paragraph) | "Relationship to DpHyp" — positions against Calcite's existing HyperGraph/DpHyp infrastructure | See Issue 2 |
| 3.4 (WCOJ Enumerator) | More detailed two-phase pseudocode with explicit backtracking correctness argument | Good — appropriate depth for systems paper |
| 5.2 (Sub-Expression Sharing) | New "Memory considerations" paragraph on spool sizes, heap limits, GC pressure | Good — preempts reviewer scalability questions |
| References | Added [28] Moerkotte & Neumann (DpHyp) | Correct; needs Zotero entry |

---

## Issues

### Issue 1 (Low-Medium): Section 3.2 Hypergraph Construction Is Over-Detailed

**Lines 187–192.** The four-step hypergraph construction procedure (field offset computation, equi-join extraction, equivalence class construction, hyperedge projection) reads like implementation documentation rather than paper prose. Two specific concerns:

**a) Algorithmic boilerplate.** The parenthetical "(with path compression and union-by-rank)" in step 3 describes a textbook data structure optimization that a database audience takes for granted. Similarly, step 1 ("computed by accumulating row-type widths") is a mechanical detail that adds no conceptual insight. These details are appropriate for Appendix A or a code comment, not the paper body.

**Suggested fix:** Collapse the four steps into two sentences:

> From the `MultiJoin`'s conjunctive filter, we extract equi-join predicates and group co-equated fields into equivalence classes using Union-Find. Each equivalence class spanning two or more inputs becomes a `JoinVariable` — a hyperedge in the join hypergraph.

This preserves the key ideas (equi-join extraction, equivalence classes, hyperedge semantics) without the procedural scaffolding.

**b) Hyperedge degree-2 caveat is buried.** Line 190 notes: "Because all join predicates in our benchmarks are binary (linking exactly two inputs), the hyperedges are all of degree 2 and the hypergraph reduces to a standard undirected graph." This raises an important question: does the *system* handle higher-degree hyperedges, or only the benchmarks? If the system supports general hyperedges but the benchmarks only exercise degree-2, say so explicitly (e.g., "The implementation handles general hyperedges, though all queries in our evaluation happen to be degree-2"). If degree-2 is a system limitation, it belongs in Section 7.2.5 (Limitations). As written, the reader cannot tell.

### Issue 2 (Low): DpHyp Paragraph Buries Its Architectural Insight

**Line 194.** The "Relationship to DpHyp" paragraph contains the paper's clearest statement of where WCOJ routing fits in the optimizer architecture: "DpHyp answers *in what order should these binary joins execute?*, while `EnumerableWCOJRule` answers *should these joins be executed as WCOJ at all?*" This is an excellent distinction — it tells the reader exactly how the two mechanisms compose.

Three sub-issues:

**a) The insight arrives late in the paragraph.** The paragraph opens with internal naming (`analyzeJoinGraph` method, `JoinGraph` vs `HyperGraph`, separate `UnionFind`). These are implementation details that a paper reader does not need. The architectural insight — DpHyp picks binary join order, our rule picks the join paradigm — should lead.

**Suggested restructuring:**

> Calcite includes DpHyp [28], Moerkotte and Neumann's DP algorithm for enumerating optimal binary join trees over a join hypergraph. Our cyclicity test serves a complementary role: DpHyp answers *in what order should these binary joins execute?*, while our rule answers *should these joins be executed as WCOJ at all?* Once the cyclicity test fires, the join ordering question is sidestepped — WCOJ processes all variables simultaneously in a fixed global ordering. A natural future direction is a hybrid strategy: DpHyp for acyclic sub-plans, WCOJ for cyclic cores (see Section 9, item 4).

**b) The `@Experimental` annotation is noise.** Noting that DpHyp is `@Experimental` in Calcite's codebase is relevant to a developer reading the source but not to a paper reader evaluating the research contribution.

**c) The future refactoring suggestion duplicates conclusion item (4).** The paragraph ends with "A natural future refactoring would unify the two constructions and allow a hybrid strategy: DpHyp for acyclic sub-plans, WCOJ for cyclic cores (see Section 9, future work item 4)." The conclusion's item (4) already says: "extending the `Combine` operator to support heterogeneous join strategies (e.g., WCOJ for cyclic components and binary joins for acyclic components within the same batch)." Having the same idea stated twice dilutes both occurrences. Options:

- Keep the forward reference in 3.2 and shorten item (4) to a back-reference: "hybrid join strategies within a batch, as motivated in Section 3.2."
- Cut the forward reference from 3.2 and let the conclusion own it.

Either works; the current duplication does not.

### Issue 3 (Low): Section 3.2 Step 1 Paragraph Is a Single Long Sentence

**Line 183.** The paragraph explaining `JoinToMultiJoinRule` is grammatically correct but dense. The core sentence runs 70+ words:

> "Calcite's standard planner represents joins as a binary tree of `LogicalJoin` nodes. Individual pairwise joins carry only their local condition (e.g., $R.b = S.b$), so cyclicity is not detectable at any single node in the tree. `JoinToMultiJoinRule` (a standard Calcite rule) collapses an inner-join subtree into a single `MultiJoin` node with $N$ inputs and a single combined condition formed by conjoining all join predicates."

This is three ideas packed together: (a) Calcite uses binary join trees, (b) cyclicity is invisible locally, (c) flattening makes it visible. The first two sentences are fine. The third sentence tries to do too much — it introduces the rule name, describes what it does, and explains its output format. Consider splitting:

> `JoinToMultiJoinRule` collapses the inner-join subtree into a single `MultiJoin` node with $N$ inputs and one combined equi-join condition. Once this flattening has occurred, all join predicates are co-located, making the global join structure accessible for cyclicity analysis.

### Issue 4 (Trivial): Reference [28] Needs Zotero Entry and Has No DOI

Reference [28] (Moerkotte and Neumann, VLDB 2006) is correctly formatted and matches the existing reference style. However:

- It has no DOI in the citation (the VLDB 2006 proceedings predate consistent DOI assignment). This is fine — [20] (Graefe, Cascades) also omits a DOI — but for consistency you may want to add an `Available:` URL if one exists.
- It is not in your Zotero library. Add it manually.

---

## Issues Resolved Since v2

All nine issues from v2 remain resolved. No regressions detected.

---

## Summary

| Priority | Issue | Effort |
|:---|:---|:---:|
| Low-Medium | Section 3.2 hypergraph construction over-detailed | Low |
| Low | DpHyp paragraph buries its insight, duplicates conclusion | Low |
| Low | Section 3.2 Step 1 long sentence | Trivial |
| Trivial | Reference [28] not in Zotero, no DOI | Trivial |

**Bottom line:** The new content is valuable — the DpHyp relationship, the backtracking correctness argument, and the memory considerations all strengthen the paper. The issues are about trimming implementation detail for a paper audience. The paper remains camera-ready; these are optional polish items.
