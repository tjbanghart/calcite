# Paper Review Notes v4: Implementation Review

**Date:** 2026-03-02
**Scope:** Review of implementation sections (3-7) of `docs/wcoj-mqo-calcite.md`. Lit review (Section 2) assessed as solid; not covered here.

---

## Substantive Concerns

### 1. Cost model (3.5) is too thin for the claims it supports
- Model: $C = \sum|R_i| + |Q(D)|$ — misses per-level intersection cost
- This omission is exactly what explains the FK regressions in 7.2.4
- Model predicts WCOJ always wins on cyclic queries, but experiments show otherwise
- Options: (a) extend model to include intersection term approximately (e.g., $\sum_k |\text{bindings}_{k-1}| \cdot \min_i |\pi_k(R_i)|$), or (b) reframe 3.5 as explicitly a "lower bound" model and forward-reference failure modes more aggressively
- Current disconnect between model and results feels like oversight rather than deliberate simplification

### 2. Combine regression on heavy queries (7.2.4) is underexplained
- Plain Combine is 30% slower than sequential WCOJ on self-join triangle
- Attribution to "plan compilation + trie construction" doesn't hold — trie caching should mean fewer tries, not more
- Is the overhead actually in MULTI() code generation (compiling single large method for N=5)? Or GC pressure from packing all results into struct row type?
- Needs more diagnosis — undermines core value proposition of Combine

### 3. Theorem 1 (6.1) has a subtle gap
- Proof assumes global variable ordering is the same across both queries
- Where is this guaranteed? If two WCOJ operators independently choose variable orderings (e.g., based on estimated cardinalities), fingerprints could match at positions 0..K-1 but over different variable sequences
- Need to state explicitly that prefix-sharing rule enforces a common variable ordering, or explain where invariant comes from in the implementation

### 4. MULTI() design alternatives (4.2) could be sharper
- Currently dismisses UNION ALL (schema compatibility) and hints (don't alter relational algebra)
- Real argument: MULTI() creates distinct semantics — independent result sets — that neither UNION ALL nor hints capture
- Current framing sounds like convenience choice rather than semantic necessity

---

## Smaller Issues

- **Section 3.4**: Transition between Phase 1 and Phase 2 in moveNext() pseudocode is unclear. Add a one-sentence bridge after Phase 1 finds the first valid complete binding.
- **Table 3, N=2**: Both batched modes have CV > 50%. Consider dropping the N=2 row entirely or moving to a footnote — adds noise without insight.
- **Section 5.2**: "The rule skips leaf table scans (handled by trie caching) and the Combine root" — explain why skipping leaf scans is correct. Reader might wonder: if two queries scan the same table with different filters, shouldn't that be shared too?
- **Section 6.2**: Savings formula $\Delta C = (N-1) \cdot P + (N-1) \cdot C_{\text{trie}} - C_{\text{coord}}$ omits spool cost from 5.2. Is $C_{\text{coord}}$ meant to absorb it? Clarify.
- **Appendix A**: Abbreviated paths (`core/.../`) inconsistent with full paths in system overview. Pick one convention.

---

## What Works Well

- **Section 3 system overview diagram** — immediately orients the reader
- **Cyclic detection logic (3.2)** — clean: flatten to MultiJoin, build hypergraph, check |E| >= |V|; DpHyp relationship is a nice touch
- **Section 5 layered structure** — table showing mechanism/scope/enabled-by is effective; explanation of why TrieCache cross-operator sharing requires spooling is credible implementation detail
- **Section 7.2.4 TPC-H honesty** — reporting failure modes (FK rectangle 1.5x slower, FK diamond 3.6x slower) with explanation is much more convincing than cherry-picked wins

---

## Overall Assessment

Implementation design is sound and well-motivated. Biggest gap is mismatch between simplified cost model and actual experimental behavior — tightening that connection would make formal analysis and experiments tell a more coherent story. Honesty about failure modes is a real strength; lean into it by making the cost model predict *when* WCOJ will lose, not just when it will win.
