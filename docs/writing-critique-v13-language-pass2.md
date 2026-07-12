# Language Critique -- Pass 2

The results section (7) and conclusion (8) now sound much more natural. The rewrites land well. But the improvement has created a new problem: **the results section now sounds noticeably different from Sections 3-6.** The technical sections are still written in clean, compressed, experienced-researcher prose, while the results section is natural and conversational. A reader (or advisor) will notice this gap.

You don't need to flatten Sections 3-6 as much as 7-8 -- technical sections *should* be more precise. But there are specific spots where the gap is jarring.

---

## The tone gap between sections

### Section 5 intro (line 211) vs. results section

Section 5 (rewritten):
> "These target different kinds of redundancy. Trie caching avoids rebuilding the same hash tries. Sub-expression sharing avoids running the same operator subtree multiple times. Prefix sharing avoids traversing the same WCOJ search space twice."

This is good -- it matches the tone of the results section. But right after, line 225:

> "A per-execution trie cache maps each `(input, keyIndex)` pair to a shared trie, built on first access."

This is back to polished mode. "Built on first access" is a clean participial phrase. More natural:

> "A per-execution trie cache stores one trie per `(input, keyIndex)` pair, building it the first time it's needed."

### Section 5.3 (line 253)
> "Detect shared prefixes at compile time via fingerprinting, compute the prefix once at runtime, and distribute the bindings to per-query suffix executors."

Imperative tricolon. The rest of the paper now avoids these. More natural:

> "The idea is to figure out shared prefixes at compile time using fingerprinting, then at runtime compute the prefix once and pass the bindings to per-query suffix executors."

### Section 4.3 (line 192)
> "The optimizer evaluates the cumulative cost through children, allowing optimization rules to improve individual queries or exploit cross-query sharing."

The "allowing X to Y" participial is polished:

> "The optimizer looks at the total cost across all children, so optimization rules can still improve individual queries or find sharing opportunities."

### Section 4.3 (line 194)
> "This is essential for MQO: the optimizer can see all queries simultaneously and identify sharing opportunities that are invisible when queries are optimized in isolation."

> "This matters because when queries are optimized one at a time, the optimizer can't see the sharing opportunities between them."

### Section 3.2 (line 86)
> "Detecting cyclic join patterns requires flattening the binary join tree so all predicates are visible simultaneously, then testing the resulting join graph for cycles."

> "To detect cyclic joins, we first need to flatten the binary join tree so all the predicates are visible at once, and then check whether the resulting graph has cycles."

---

## Remaining spots in the results section

Most of 7.2 reads well now. A few leftovers:

### Line 414: "size-dominated" / "batch-dominated"

> "(size-dominated) and ... (batch-dominated)"

These are coined compound adjectives that feel like researcher shorthand. More natural:

> "(where graph size matters most) and ... (where batch size matters most)"

### Line 456
> "We additionally test two FK-based cyclic queries"

Minor: "additionally" → "also"

---

## Abstract

The abstract is the one place where polish is expected. It's fine to leave it more formal. But one phrase stands out:

### Line 3
> "We also characterize failure modes: on FK cycles with highly selective closing predicates, binary joins outperform WCOJ by up to 3.9x."

"Characterize failure modes" is clinical. Slightly more natural:

> "We also show where WCOJ doesn't help: on FK cycles with highly selective closing predicates, binary joins outperform WCOJ by up to 3.9x."

---

## Sections 2 and 6

### Section 2.5 (line 65) is inconsistent with the rewritten intro

The intro (line 18) now has the natural version:
> "tries can be reused across queries, identical parts of the search don't need to be traversed more than once, and when queries share the same variable prefix, that prefix can be computed once and handed off to per-query suffix executors"

But Section 2.5 (line 65) still has the polished version of the same idea:
> "tries can be shared across queries, identical search-space traversals can be deduplicated, and shared variable prefixes can be factored out"

These are saying the same thing in two different voices. Either rewrite line 65 to match the intro's tone, or (simpler) just cut line 65 since Section 2.5 is short and the final sentence "Section 5 describes the three optimizations that exploit these opportunities" is enough of a bridge.

### Section 2.3 (line 55)
> "We take a different approach, exploiting WCOJ's structure directly rather than solving a selection problem."

"Exploiting X directly rather than Y" is efficient:

> "Instead of solving a selection problem, we take advantage of WCOJ's structure directly."

### Section 6.2 (line 343)
> "so the net benefit requires $N$ large enough for the $(N{-}1) \cdot P$ savings to dominate"

> "so sharing only pays off once $N$ is big enough that the $(N{-}1) \cdot P$ savings outweigh the overhead"

---

## Conclusion future work (line 503)

The first two sentences are great now. But the rest of the paragraph reverts to polished mode:

> "Variable ordering currently follows syntactic predicate order, but an adaptive strategy that accounts for cross-query prefix sharing could improve both single-query and batched performance."

Two ideas in one sentence with a "but" pivot:

> "Variable ordering just follows the order predicates appear in the SQL right now. An adaptive strategy that also considers cross-query sharing could do better for both single-query and batched performance."

> "Longer-term, integrating with Calcite's materialized view subsystem would enable persistent sharing across batches, and hybrid plans that route cyclic components to WCOJ while keeping acyclic components on binary joins would extend the framework's applicability."

Two independent ideas joined by "and," each with subordinate clauses:

> "Further out, integrating with Calcite's materialized view subsystem could make sharing persist across batches. Hybrid plans that use WCOJ for cyclic parts while keeping binary joins for acyclic parts would also make the framework more broadly useful."

---

## Summary

The paper is in good shape overall. The main remaining issue is the **tone gap between the natural-sounding results/conclusion and the still-polished Sections 3-6**. You don't need to flatten everything -- technical sections should be precise -- but the specific spots I flagged above are where the gap is most noticeable. Maybe 8-10 sentence-level edits across Sections 2-6 would bring things into alignment.
