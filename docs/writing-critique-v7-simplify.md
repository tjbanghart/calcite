# Writing Critique v7: Simplify and Sound Human

**Date:** 2026-03-15
**Scope:** Critique of `wcoj-mqo-calcite-v2.md`. Too complicated and verbose. Needs to sound like an M.S. student, not a PhD who has been doing this for years.

---

## Overall Diagnosis

The paper reads like it's trying to prove it belongs at VLDB rather than clearly explaining what you built. The core ideas are genuinely good, but they're buried under defensive hedging, over-qualification, and sentences that do three jobs at once. A reader shouldn't need to parse 50-word sentences to understand "we cache tries so we don't build them twice."

---

## 1. Abstract - Tries to Do Everything

The abstract is 180 words and packs in: problem statement, algorithm name, system name, SQL syntax, three optimization names, two benchmark suites, four quantitative results, AND a failure mode characterization. That's a conference talk, not an abstract.

**Problem sentence:**
> "Cyclic join queries (triangle counting, graph motif search, and multi-hop correlation patterns) are intractable for binary join engines. No join ordering avoids intermediate results that can be exponentially larger than the output."

This is two sentences saying the same thing. Pick one.

**The contribution sentence is 95 words long:**
> "Our framework introduces the `Combine` relational operator and `MULTI()` SQL syntax for declarative query batching, together with three cross-query optimizations that exploit WCOJ's variable-at-a-time structure: identity-based trie caching, frequency-aware sub-expression sharing via spools, and shared-prefix execution through join-variable fingerprinting."

Break this up. Name the optimizations in the contributions list, not crammed into a single clause.

---

## 2. Related Work (Section 2) - Way Too Thorough

This is the biggest offender. Section 2 is ~1800 words. For an M.S. paper, you're spending too much real estate showing you've read everything. Specific issues:

**Section 2.1** explains the AGM bound formula, the fractional edge cover LP, *and* lists four follow-up theoretical papers. You only use Generic-Join. Cut the LP details and the "subsequent work extended the theoretical landscape" paragraph. A sentence saying "the AGM bound [2] gives a tight upper bound on join output size; Generic-Join [3] achieves it" is sufficient.

**Section 2.2** has six subsections for related systems. The Ring paragraph alone is 100 words explaining wavelet trees and cyclic rotations of triples for arity d=3. This is fascinating but irrelevant to your actual contribution. Table 1 already makes the comparison; the prose should be 2-3 sentences per system, not mini-literature-reviews.

**Section 2.4** (Query Optimization Frameworks) recaps System R, Volcano, and Cascades history. Your reader knows this. The sentence "Apache Calcite [9] implements a hybrid Volcano/Cascades optimizer and serves as the query processing backbone for Apache Hive, Apache Flink, Apache Druid, Trino, and numerous other systems" is the only one you need. Delete the rest.

**Section 2.5** ("The Unexplored Intersection") is good conceptually but over-explains. Each of the three numbered items could be one sentence instead of a full paragraph with contrasting binary-join behavior.

---

## 3. Hedging and Over-Qualification

The paper is full of defensive language that weakens rather than strengthens:

> "We deliberately present this as a lower bound rather than a complete cost model."

Just say "This is a lower bound." The reader can see it's deliberate.

> "This is an acceptable tradeoff for our setting"

Don't apologize for design decisions. State the tradeoff and move on.

> "The experimental results point toward a broader lesson."

Just state the lesson.

> "The real value of the Combine framework is providing a *platform for selective optimization*"

This phrase ("platform for selective optimization") appears three times in the paper. Once is enough.

---

## 4. Sentences That Try Too Hard

Many sentences pack in parentheticals, asides, and qualifications that make them hard to parse:

> "Spooling materializes intermediate results in memory. For the TPC-H self-join triangle at SF=0.01, each spool holds up to 2.9 million rows (~100-200 MB depending on projection width). The current implementation does not spill spools to disk; disk-backed spooling is a natural extension for production use."

The last clause ("a natural extension for production use") is throat-clearing. Just say you don't spill to disk yet.

> "Each equivalence class spanning two or more inputs becomes a `JoinVariable`, a hyperedge in the join hypergraph."

This redefines something the reader already knows from Section 2. Trust your earlier explanation.

> "Our work does not address the general MQO selection problem [10]; it exploits the specific structure of WCOJ execution to identify sharing opportunities within batches of cyclic join queries."

This appears in both the related work AND the conclusion. Once is enough.

---

## 5. Section 6 (Formal Analysis) - Overkill for the Contribution

The correctness proof (Theorem 1) has three conditions (a, b, c), a "variable ordering invariant" paragraph, AND a proof sketch. For prefix sharing, the intuition is simple: if two operators process the same variables over the same data in the same order, they produce the same bindings. The formalism adds rigor but the surrounding prose could be halved.

The cost analysis (Section 6.2) is clean and can stay mostly as-is.

---

## 6. Experimental Analysis - Explains Too Much

The analysis paragraphs after each table are thorough but repetitive. For example, Section 7.2.1:

> "Speedup over baseline increases from 1.5x at |V|=50 to 3.2x at |V|=400, demonstrating that the advantage compounds as the join graph becomes denser."

The table already shows this. You can just say "WCOJ's advantage grows with graph density (1.5x to 3.2x)."

Section 7.2.4's analysis is 450+ words. The key insights are: (1) WCOJ wins on self-joins because it avoids intermediate blowup, (2) WCOJ loses on FK cycles because selective closing predicates let binary joins prune early, (3) Combine has materialization overhead. That's three sentences. The rest is restating numbers from the table.

---

## 7. Specific Language Patterns to Fix

| Pattern | Example | Fix |
|---------|---------|-----|
| Passive hedging | "It should be noted that..." | Delete |
| Redundant definitions | "a sub-plan is a node and its descendants in the relational operator tree -- e.g., a WCOJ operator together with its input table scans" | Pick the example OR the definition |
| Triple-stacked citations | "Sellis [6] and Finkelstein [23], who established..." then "Kathuria and Sudarshan [7]..." then "Jindal et al. [25]..." | Group into one sentence |
| Restating what a figure shows | "Figure 3 shows that speedups grow consistently as graph size increases" then describing the same data | Trust the figure |
| "Qualitatively different" | Used 3 times | Say "different" |

---

## Summary of Recommendations

1. **Cut Section 2 by 40%.** Table 1 does the heavy lifting. Trim each system to 1-2 sentences. Delete the System R / Volcano / Cascades history.
2. **Shorten the abstract** to ~120 words. One problem sentence, one approach sentence, three result highlights.
3. **Remove repeated framing.** "Platform for selective optimization" once. "Does not address the general MQO selection problem" once. The FK failure mode explanation once (in 7.2.4, not also the conclusion).
4. **Trust your tables and figures.** Cut analysis paragraphs that restate numbers already in the table by ~50%.
5. **Kill hedging language.** "We deliberately...", "This is an acceptable tradeoff...", "It is worth noting..." -- just state things directly.
6. **Simplify the proof.** The intuition paragraph before Theorem 1 could replace the formal statement for an M.S. paper. If you keep the formal version, cut the surrounding explanation.

The technical content and experimental design are solid. The paper just needs to get out of its own way.
