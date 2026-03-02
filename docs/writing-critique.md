# Writing Critique: "Worst-Case Optimal Joins Meet Multi-Query Optimization"

**Date:** 2026-02-25
**Scope:** Prose quality, structure, rhetoric, clarity, and style. Not scientific content (covered in peer-review-v1 through v5).

---

## Overall Assessment

The paper is technically strong but reads like an **engineering report with theoretical dressing** rather than a research paper with a clear argumentative arc. The writing is competent — no grammatical errors, no ambiguous antecedents, no dangling modifiers — but it lacks the rhetorical sharpness that distinguishes memorable systems papers. The main issues are: (1) the introduction front-loads mechanism over motivation, (2) the related work section is exhaustive but passive, (3) the experimental analysis over-explains obvious points while under-developing the interesting ones, and (4) the prose defaults to list-and-describe when it should argue-and-convince.

**Grade: B+.** Publishable as-is, but a revision focused on tightening the narrative could elevate it to an A.

---

## Section-by-Section Critique

### Abstract (lines 9)

**Problem:** The abstract is a single 180-word sentence-block that reads like a feature list. It says what you built but not *why it matters* or *what you found*.

The phrase "To our knowledge, our approach is the first to combine WCOJ algorithms with multi-query optimization in an open-source, general-purpose SQL framework" is the most important sentence in the abstract and it's buried at position 6 of 7. Lead with it.

**Missing:** A single concrete result number. Abstracts for systems papers should include at least one headline number (e.g., "achieving up to 4x speedup" or "enabling queries that binary joins cannot complete within memory limits"). The reader should know *what happened*, not just what you built.

**Suggestion:** Restructure as: (1) problem statement (2 sentences), (2) key insight / novelty claim, (3) what you built (brief), (4) what you found (1–2 headline results).

### 1. Introduction (lines 13–35)

**Strengths:** The triangle query example (line 17–21) is well-chosen and clearly explained. The $O(|E|^2)$ intermediate blowup vs. $|E|^{3/2}$ AGM bound is the right hook.

**Problems:**

1. **The contributions list is generic.** Contribution #1 says "We implement a hash-based WCOJ algorithm with multi-level trie indexing." This describes *what* you did, not *why it's a contribution*. Compare: "We show that hash-based WCOJ can be integrated into a Volcano-based optimizer without requiring pre-sorted indices or schema changes, enabling adoption by the >20 systems built on Apache Calcite." The second version tells the reader why they should care.

2. **The MQO motivation is weak.** Line 23 says MQO "addresses the problem of redundant computation when multiple queries share common sub-expressions." This is textbook boilerplate. The real motivation — which appears much later in Section 5 — is that WCOJ's variable-at-a-time structure creates *qualitatively different* sharing opportunities (trie sharing, search-space sharing, prefix factoring) that don't exist in binary-join workloads. This insight should be in the introduction, not buried in Section 2.5.

3. **No forward reference to the failure modes.** The introduction promises only benefits. A sophisticated reader will be skeptical. Adding one sentence — "We also characterize the failure modes: WCOJ regresses on FK-based cycles where binary joins handle selective closing predicates efficiently" — would build credibility upfront.

4. **The roadmap paragraph (line 35) is dead weight.** "Section 2 surveys related work. Section 3 describes..." — this is a 1990s convention that wastes space. The reader can see the section headers. Cut it.

### 2. Background and Related Work (lines 39–123)

**Strengths:** Thorough. Table 1 is excellent — it makes the positioning claim falsifiable at a glance. The "Unexplored Intersection" subsection (2.5) is the best-written part of the paper.

**Problems:**

1. **Too much passive summarization.** Sections 2.1–2.4 read like a literature survey rather than a motivated review. Each paragraph follows the pattern: "[Author] [year] did [X]." This is correct but inert. Better: frame each piece of prior work in terms of the gap your paper fills. Example: instead of "Freitag et al. [5] made the critical observation that WCOJ can be practical within a general-purpose RDBMS without requiring pre-sorted indices," try "Freitag et al. [5] showed that WCOJ can work inside a general-purpose RDBMS — but only for single queries. Their system has no mechanism for cross-query sharing."

2. **Section 2.3 (MQO) is too long for its purpose.** You cite 8 MQO papers but explicitly disclaim solving the general MQO selection problem (line 97). The reader is left wondering why they just read 500 words about NP-hardness and submodular maximization if it's not what the paper does. Trim 2.3 to the 3–4 works most relevant to your structural approach, and redirect the saved space to 2.5 (which deserves expansion).

3. **Section 2.5 is too short.** This is the intellectual core of the paper's motivation — *why* WCOJ+MQO is a natural combination — and it gets just 8 lines. The three sharing opportunities (trie sharing, search-space sharing, variable-level factoring) each deserve a concrete example. Currently they're stated as abstract claims. Showing a 3-line example of how two triangle queries traverse identical search trees would make the insight visceral rather than theoretical.

### 3. WCOJ Integration in Calcite (lines 127–225)

**Strengths:** Section 3.2 (cyclicity test) is crisp — the $|E| \geq |V|$ observation is stated cleanly with the right caveat about connected graphs. Section 3.4 (WCOJ Enumerator) is technically clear.

**Problems:**

1. **Section 3.1 opens with bureaucracy.** "We introduce `EnumerableWCOJ`, a physical operator in the enumerable convention..." — this tells you about Java class hierarchy before it tells you about the algorithm. Lead with the idea, then name the class. Better: "Our WCOJ operator takes $N \geq 3$ inputs and processes them simultaneously via multi-way intersection, unlike binary join operators that combine exactly two inputs."

2. **The JoinVariable notation is over-formalized.** The equation $\text{JoinVariable}(v) = \{(i_1, f_1), (i_2, f_2), \ldots\}$ adds nothing over the triangle example that follows it. The example alone is clear. Cut the equation or move it to an appendix.

3. **Section 3.3 (Hash Tries) lacks a comparison.** You mention sorted tries (Veldhuizen) and hash tries (Freitag) but don't explain the tradeoff in your own words. What do you give up by using hash tries? (Answer: sorted enumeration, which matters for leapfrog but not for your intersection approach.) One sentence would suffice.

4. **Section 3.5 (Cost Model) is honest but abrupt.** The limitations paragraph is well-written — the FK regression explanation is clear — but the transition "A more complete cost model... is left for future work" is a sudden dead end. Consider framing it as: "Section 7.2.4 provides empirical evidence for when this model breaks down."

### 4. Combine Operator (lines 228–278)

**Strengths:** Section 4.2's "Design alternatives" paragraph is excellent — it considers UNION ALL and hints, explains why they don't work, and justifies the chosen approach. This is exactly the kind of design reasoning that makes a systems paper convincing.

**Problems:**

1. **Section 4.1 (Motivation) is redundant.** "When a workload consists of multiple queries over the same data... traditional engines execute them independently." This repeats the introduction word-for-word. Either cut 4.1 entirely and open Section 4 with the SQL extension, or add a *new* observation here (e.g., how many tries get built redundantly for a 20-query batch).

2. **The row type equation (line 260) is intimidating for a simple concept.** The struct-of-arrays idea is just "each output field is a list of results from one sub-query." Say that in English, then optionally give the formal type. Currently the equation comes first and the explanation comes second — reverse the order.

3. **"Novelty" as a subsection header (line 266) is self-congratulatory.** Let the reader judge novelty. Rename to "Relationship to existing operators" or fold into the preceding paragraph.

### 5. Cross-Query Optimizations (lines 281–401)

**Strengths:** The table at line 285–289 is excellent — it makes the three optimizations immediately distinguishable. Section 5.3 (prefix sharing) is the most technically interesting part of the paper and is explained well.

**Problems:**

1. **Section 5.1 (Trie Caching) spends too long on Java details.** The `IdentityHashMap` and `computeIfAbsent` code snippet is implementation detail, not algorithmic insight. The key idea — "same Java object = same trie, lazy construction, scoped to one Combine execution" — can be stated in 3 sentences. The code adds visual clutter without conceptual content.

2. **Section 5.2 overuses the Before/After diagram format.** The tree diagram is helpful but the surrounding text repeats what the diagram already shows. "The rule creates a LogicalTableSpool that materializes the result once and replaces subsequent occurrences with consumer scans" — this is exactly what the diagram says. Cut one.

3. **The "Scope of sharing" paragraph (lines 319–323) in Section 5.1 is critically important but buried.** This paragraph explains that TrieCache alone does NOT provide cross-operator sharing — you need the CombineSharedComponentsRule for that. This is the key subtlety that distinguishes "combine" from "combine-share" mode, and it's hidden in a paragraph that starts with "The identity-based design means that..." Move this earlier or give it a bold callout. It directly explains the experimental results.

### 6. Formal Analysis (lines 405–437)

**Strengths:** The proof sketch is appropriately informal for a systems paper. The cost analysis is clear and directly connects to experimental predictions.

**Problems:**

1. **Theorem 1's statement is imprecise for a theorem.** "Let $Q_1$ and $Q_2$ be two WCOJ queries with variable orderings $(x_1, \ldots, x_m)$ and $(x_1, \ldots, x_K, y_{K+1}, \ldots, y_{m'})$ respectively, such that variables $x_1, \ldots, x_K$ have identical fingerprints." This mixes notation ($x_k$ vs $y_k$) and doesn't define "identical fingerprints" within the theorem statement. A formal theorem should be self-contained. Either make it fully formal (define fingerprint equality inline) or label it as a "Claim" or "Proposition" rather than a "Theorem."

2. **The proof sketch's key step is hand-waved.** "Given these shared trie objects, the candidate sets are identical at each prefix level, and the search trees are isomorphic up to depth $K$." The isomorphism claim is the whole point of the theorem, and it's asserted rather than argued. One more sentence explaining *why* identical candidate sets imply identical search trees (induction on $k$: same candidates at level $k$ means same branches, each branch leads to same sub-problem at level $k+1$) would close the gap.

3. **The cost analysis mixes constants and asymptotic terms.** $C_{\text{coord}}$ is described as both "constant-factor" (line 431) and "proportional to the output size" (line 437). These are contradictory. If it's proportional to output size, it's not constant — it's $O(|Q(D)|)$. Clarify which one.

### 7. Experimental Evaluation (lines 441–582)

**Strengths:** The analysis paragraphs are detailed and honest. The variance discussion (CV markers, JIT effects, OS page cache) shows methodological awareness that many systems papers lack. The FK regression analysis (lines 575–581) is the best analytical writing in the paper.

**Problems:**

1. **The analysis is too long and too homogeneous.** Every result gets the same depth of treatment: a bullet-pointed decomposition of what happened. But not all results are equally interesting. The 4-cycle OOM result deserves a paragraph; the 50-node triangle result (86ms vs 58ms) does not. Currently both get similar space. Prioritize.

2. **"Several trends emerge" and "The results reveal" are filler phrases.** These introduce analysis paragraphs without saying anything. Cut them and start with the trend itself: "Per-query amortized cost drops from 105 ms at $N=2$ to 11 ms at $N=20$..."

3. **Table 4 (Optimization Contributions) adds little.** It repeats data from Tables 2 and 3 at two cherry-picked operating points. The "What's Active" column just restates the mode definitions from Table at line 447. If the point is to show an ablation, the text already makes this argument. Consider cutting Table 4 and folding its insight into the Table 2/3 analysis.

4. **The long parenthetical in line 575 is hard to parse.** This is a 150-word parenthetical inside an analysis paragraph, covering variance, GC pressure, page cache, outlier retention policy, and uncertainty estimates. Break it into its own paragraph or a footnote.

5. **Missing: a limitations paragraph at the end of Section 7.** The paper discusses individual result limitations inline, but there's no consolidated discussion of: (a) single-machine only (no distributed evaluation), (b) in-memory spools with no disk spillover, (c) SF=0.01 is tiny, (d) the workload is synthetic projections over the same join structure rather than truly heterogeneous queries. These are all acknowledged in passing but should be collected so the reader can assess the scope of the claims at a glance.

### 8. Conclusion (lines 585–595)

**Problem:** The conclusion restates results without reflecting on them. A good conclusion should say something *new* — a lesson learned, a surprising finding, or a reframing of the contribution in light of the results.

The most interesting insight from the experiments — that WCOJ is not universally beneficial, and that the real value of the Combine framework is providing a *platform* for selective optimization (WCOJ where it helps, binary joins where it doesn't) — is absent from the conclusion. Instead, the conclusion lists peak speedup numbers that don't represent the full story.

**Suggestion:** Add 2–3 sentences after the results summary: "Our results suggest that the primary value of integrating WCOJ into a general-purpose optimizer is not universal speedup but *selective applicability*: the optimizer can route cyclic sub-queries to WCOJ while leaving acyclic components to binary joins, and the Combine framework amortizes the cost of both strategies across the batch. Future work item (4) — hybrid join strategy within a batch — directly addresses this."

### Future Work (line 595)

The four directions are well-chosen but item (1) — "reducing coordination overhead through direct binding propagation rather than materialization" — is vague. What does "direct binding propagation" mean concretely? A single clarifying clause would help (e.g., "passing prefix bindings via shared memory rather than materializing them as intermediate result sets").

---

## Prose-Level Issues

### Recurring Patterns

1. **Overuse of em-dashes.** The paper uses em-dashes 30+ times. They're effective for asides but lose impact when overused. Replace half of them with parentheses or restructure as separate sentences.

2. **Passive voice in technical sections.** "The rule fires only when..." / "The cost is modeled as..." / "The trie supports three operations..." — passive voice is fine for one-off descriptions but creates a monotonous rhythm when every paragraph starts this way. Vary the sentence structure.

3. **Hedging where confidence is warranted.** "To our knowledge, our approach is the first..." — if you've done a thorough literature review (you clearly have), say "Ours is the first" or "No prior work has combined...". The hedge weakens the claim without adding honesty (you'd still be wrong if someone else did it first, regardless of the hedge).

4. **Overuse of bold for emphasis.** Bold text appears in almost every analysis paragraph. When everything is emphasized, nothing is. Reserve bold for the 2–3 most important findings per section.

### Sentences That Need Rewriting

- **Line 9 (abstract):** "Modern analytical workloads increasingly feature batches of structurally similar queries over the same data, many involving cyclic join patterns such as triangle counting, clique detection, and graph motif search." — This is a 30-word sentence that says three things. Split it.

- **Line 252:** "The extension is implemented entirely within Calcite's parser and SQL-to-Rel layer (two files: Parser.jj and SqlToRelConverter.java), with no changes to the validator or type system." — Good content, but the parenthetical "(two files: ...)" breaks the flow. Make it a separate sentence: "The implementation touches only two files: `Parser.jj` and `SqlToRelConverter.java`."

- **Line 575:** The 150-word parenthetical starting with "(The triangle and 4-cycle baseline measurements exhibit high variance..." — This is too much information for a parenthetical. Promote to a full paragraph or footnote.

---

## Structural Suggestions

1. **Merge Sections 4.1 and the end of Section 2.5.** Both motivate the same thing (why WCOJ+MQO is natural). Having the motivation in two places dilutes both.

2. **Add a "System Overview" figure after the introduction.** The paper describes many components (EnumerableWCOJ, Combine, TrieCache, CombineSharedComponentsRule, prefix analyzer) but never shows how they fit together. A single architecture diagram showing the data flow from SQL → parse → optimize → execute, with callouts for each new component, would dramatically improve readability.

3. **Move Appendix A (Key Source Files) to the introduction or Section 3.** The source file table is useful context that a reader wants *early*, not after the references.

---

## Summary of Recommendations

| Priority | Issue | Effort |
|:---|:---|:---:|
| High | Add system overview / architecture figure | Medium |
| High | Restructure abstract: lead with novelty claim, add headline result | Low |
| High | Strengthen contributions list (why, not just what) | Low |
| High | Add consolidated limitations paragraph to Section 7 | Low |
| Medium | Trim Section 2.3 (MQO survey), expand Section 2.5 (intersection) | Medium |
| Medium | Fix Theorem 1 precision: define fingerprint equality, clarify proof | Low |
| Medium | Consolidate the 150-word parenthetical in Section 7.2.4 | Low |
| Medium | Rewrite conclusion to reflect on selective applicability insight | Low |
| Low | Remove roadmap paragraph from introduction | Trivial |
| Low | Reduce em-dash and bold usage | Low |
| Low | Cut Table 4 or justify its existence | Low |
| Low | Rename "Novelty" subsection header in Section 4.3 | Trivial |
