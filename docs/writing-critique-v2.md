# Writing Critique v2: "Worst-Case Optimal Joins Meet Multi-Query Optimization"

**Date:** 2026-02-26
**Scope:** Follow-up to `docs/writing-critique.md`. Assesses revisions and identifies remaining issues.

---

## Overall Assessment

**Grade: A-.** The revision addressed nearly every high-priority item from v1. The paper now reads as a research paper with a clear argumentative arc, not an engineering report. The abstract leads with the problem and novelty claim, the contributions explain *why* not just *what*, the introduction foreshadows failure modes, the conclusion reflects on selective applicability, and the system overview diagram (ASCII, Section 3.0) provides the missing architectural context. The remaining issues are minor.

---

## Issues Resolved Since v1

| v1 Issue | Priority | Status |
|:---|:---:|:---:|
| Abstract buries lede, no result numbers | High | ✅ Restructured: problem → novelty → system → results → failure modes |
| Contributions list says *what* not *why* | High | ✅ Each contribution now explains significance |
| No forward ref to failure modes in intro | High | ✅ Line 35: "We also identify the limits..." paragraph added |
| Add system overview / architecture figure | High | ✅ Section 3.0 ASCII pipeline diagram |
| Add consolidated limitations to Section 7 | High | ✅ Section 7.2.5 with four bullet points |
| Trim Section 2.3, expand Section 2.5 | Medium | ✅ 2.3 cut from ~500 words to ~200; 2.5 now has concrete examples for all three sharing types |
| Fix Theorem 1: define fingerprint, close proof gap | Medium | ✅ Condition (b) added to theorem statement; induction step explicit |
| $C_{\text{coord}}$ constant vs. $O(\|Q(D)\|)$ contradiction | Medium | ✅ Now consistently described as $O(\|Q(D)\|)$ |
| Break up 150-word parenthetical in 7.2.4 | Medium | ✅ Promoted to its own paragraph ("A note on TPC-H variance") |
| Rewrite conclusion for selective applicability | Medium | ✅ New paragraph on "platform for selective optimization" |
| Remove roadmap paragraph | Low | ✅ Gone |
| Rename "Novelty" subsection header | Low | ✅ Now "Relationship to existing operators" |
| Section 3.1 opens with bureaucracy | Low | ✅ Now leads with the idea, then names the class |
| JoinVariable over-formalized | Low | ✅ Equation removed; example alone remains |
| Section 3.3 lacks hash vs. sorted trie tradeoff | Low | ✅ New sentence on sorted enumeration tradeoff |
| Section 3.5 "left for future work" dead end | Low | ✅ Now forward-references Section 7.2.4 |
| Section 4.1 redundant with intro | Low | ✅ Rewritten with concrete 20-query redundancy numbers |
| Row type equation before English explanation | Low | ✅ English first, then formal type |
| Filler phrases ("Several trends emerge") | Low | ✅ Analysis now leads with the finding |
| Future work item (1) vague | Low | ✅ Clarified: "passing prefix bindings via shared memory rather than materializing them as intermediate result sets" |
| "To our knowledge" hedge | Low | ✅ Now "We present the first system to combine..." |

---

## Remaining Issues

### 1. Section 2.1 Still Reads as Passive Survey

**Severity:** Low-Medium

The revision improved Sections 2.3 and 2.5 substantially, but 2.1 (WCOJ Theory) and 2.2 (Practical Implementations) still follow the "[Author] did [X]" pattern without connecting each work to the gap this paper fills. For example, line 73 still reads:

> "Veldhuizen [4] introduced *Leapfrog Triejoin* (ICDT 2014), the first practical WCOJ implementation. The key insight is that sorted tries enable a *leapfrog* intersection primitive..."

This is accurate but passive. A gap-oriented framing would be:

> "Veldhuizen's *Leapfrog Triejoin* [4] demonstrated that WCOJ is practical, but requires pre-sorted tries — a constraint our hash-based approach eliminates."

The same pattern applies to EmptyHeaded (line 75: "does not support general SQL workloads" is stated as a fact rather than positioned as a gap). Freitag et al. (line 77) is better — it explicitly notes they "did not consider multi-query optimization" — but could go further.

This is a minor issue because Table 1 already positions the work clearly, and Section 2.5 now makes the gap argument well. But the prose in 2.1–2.2 still reads more like a textbook survey than a motivated argument.

### 2. Table 4 Still Adds Little Value

**Severity:** Low

The v1 critique noted Table 4 repeats data from Tables 2 and 3 at cherry-picked operating points. This was not addressed. The table still exists and the "What's Active" column still just restates mode definitions from the mode table in Section 7.1. The analysis paragraph (lines 559–563) adds useful synthesis, but the table itself could be cut without information loss — the reader can look at Table 2's |V|=400 row and Table 3's N=10 row themselves.

If the table stays, consider adding a "Marginal improvement" column showing the delta each optimization layer contributes (e.g., "WCOJ: 3.2x → Combine: +0.6x → Share: +0.2x") to make the ablation structure visually explicit rather than requiring the reader to compute ratios mentally.

### 3. TrieCache Code Snippet Is Still Implementation Detail

**Severity:** Low

The v1 critique noted the Java code in Section 5.1 (lines 337–349) adds visual clutter without conceptual content. The key idea is now better communicated by the "Important" callout (line 351), which is excellent. But the `IdentityHashMap`/`computeIfAbsent` snippet is still there. It's 12 lines of Java that a reader can find in Appendix A. The section would be tighter without it — the three-bullet summary (identity-based lookup, lazy construction, lifecycle) at lines 358–361 already says everything the code says.

This is a judgment call: some readers like code, some find it noisy. If the target venue is SIGMOD/VLDB, the code is fine (systems papers routinely include short snippets). If targeting a more theoretical venue, cut it.

### 4. Section 5.2 Text Still Repeats the Diagram

**Severity:** Low

Line 367 says "For each shared sub-tree, the rule creates a `LogicalTableSpool` that materializes the result once; subsequent occurrences become consumer scans of that spool:" — this is exactly what the Before/After diagram shows. The sentence could be cut or reduced to "The transformation is illustrated below:" and let the diagram speak.

### 5. Bold Overuse in Section 7 Analysis

**Severity:** Low

The v1 critique noted bold overuse. The revision reduced it somewhat, but Section 7.2.1's analysis (lines 515–523) still bolds four items in four bullet points. When every bullet has bold text, it's visual noise rather than emphasis. Consider bolding only the most important finding per subsection — e.g., just the 4.0x Combine-Share result at |V|=400, not every speedup number.

### 6. Table 6 Still Omits Combine/Combine-Share Data

**Severity:** Low (raised in peer-review-v5)

The CSV contains combine and combine-share data for FK rectangle and FK diamond. Adding a one-sentence note after Table 6 ("Combine and Combine-Share exhibit similar regressions; full data is archived in the benchmark CSV.") would preempt the obvious reader question. This is not about the data being missing — it's about the paper being silent on what the data shows.

---

## New Observations (Not in v1)

### 7. Section 3.0 Diagram Could Be More Informative

The ASCII pipeline diagram (lines 127–167) is a welcome addition. However, it currently shows only the *pipeline stages*, not the *data flow relationships* between new components. Specifically:

- The diagram doesn't show that `TrieCache` is created by `EnumerableCombine` and consumed by `WCOJEnumerator` — this is the most important cross-component relationship in the system.
- The "[Combine-Share only]" annotation is good but could be a visual distinction (e.g., dashed box or gray text) rather than a bracket.

For a camera-ready version with a real figure (not ASCII), consider a component-and-arrow diagram where arrows represent data/object flow rather than just the pipeline order.

### 8. The "Selective Optimization" Insight Deserves More

The conclusion's new paragraph (line 640) about "platform for selective optimization" is the paper's most mature insight — and it arrives only at the very end. This idea could be seeded earlier: perhaps a sentence in Section 4.1 (motivation) noting that the `Combine` framework is agnostic to the join strategy used by individual sub-queries, making it a natural platform for hybrid optimization. This would set up the experimental finding (FK-based queries prefer binary joins) as confirming a design goal rather than revealing a limitation.

### 9. The "materalized" Typo

Line 113: "a materalized intermediate $R \bowtie S$" — should be "materialized."

---

## Summary

| Priority | Issue | Effort |
|:---|:---|:---:|
| Low-Medium | Sections 2.1–2.2 still passive survey style | Medium |
| Low | Table 4 adds little value | Low |
| Low | TrieCache code snippet is optional | Trivial |
| Low | Section 5.2 text repeats diagram | Trivial |
| Low | Bold overuse in Section 7 analysis | Low |
| Low | Table 6 missing combine/combine-share note | Trivial |
| Low | Section 3.0 diagram could show data flow | Medium (for real figure) |
| Low | Selective optimization insight could be seeded earlier | Low |
| Trivial | "materalized" typo on line 113 | Trivial |

**Bottom line:** The paper is in strong shape. All remaining issues are low priority. The revision transformed the abstract, introduction, and conclusion from functional to persuasive, and the structural additions (Section 3.0, Section 7.2.5, expanded 2.5) substantially improved readability. Fix the typo, optionally address 1–2 of the low-priority items, and it's ready.
