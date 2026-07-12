# Writing Critique v8: Finishing Touches

**Date:** 2026-03-15
**Scope:** Second-pass critique of `wcoj-mqo-calcite-v2.md`. The big structural issues from v7 are fixed. What remains is a layer of PhD voice in individual paragraphs.

---

## Overall

The paper is in good shape structurally. The related work is tight, the analysis sections are concise, the proof is short. What's left is a pattern where individual paragraphs still read like a seasoned researcher explaining things from both directions, hedging design choices, or defining concepts the reader already knows. These are finishing touches, not rewrites.

---

## 1. Introduction Still Opens Like a Textbook

> "Join processing is the most critical operation in relational query evaluation. For decades, relational database management systems (RDBMSs) have relied on *binary join trees*: plans composed of pairwise hash joins, merge joins, or nested-loop joins arranged in a tree where each internal node combines two inputs [1]."

"For decades, RDBMSs have relied on..." is grand historical sweep. An M.S. student would start closer to the problem:

> "Standard query engines process joins in pairs. For acyclic queries this works well, but for cyclic queries like triangles, no pairwise join ordering avoids intermediate blowup [2, 3]."

You don't need to define binary join trees from first principles. Your reader knows what a hash join is.

---

## 2. Contribution 4 is Filler

> "4. **Experimental evaluation.** We evaluate on synthetic graph workloads and TPC-H cyclic joins, characterizing both speedups and failure modes."

Every paper has experiments. Listing it as a numbered contribution adds no information. Either cut it or fold it into contribution 3 ("...and evaluate on synthetic and TPC-H workloads").

---

## 3. Section 2.3 (MQO) is Still Citation-Heavy

> "The problem was formalized by Sellis [6] and Finkelstein [23], who established MQO as a *selection problem*: choosing which intermediate results to materialize. The general selection problem is NP-hard; Kathuria and Sudarshan [7] achieved a constant-factor approximation by casting materialization selection as submodular maximization, and Jindal et al. [25] demonstrated its importance at datacenter scale. Zinchenko and Ponomaryov [10] survey the broader landscape."

Five citations in three sentences covering work you explicitly say you don't use. Replace with:

> "MQO is typically formulated as a materialization selection problem [6, 7, 10, 23, 25], which is NP-hard. We take a different approach, exploiting WCOJ's structure directly rather than solving a selection problem."

---

## 4. Section 3.0 Promises Five Components, Delivers a Figure

> "The system adds five new components to Calcite's standard pipeline:"

Then a figure. What are the five? Either list them or drop the count. A reader scanning for the five will be confused when they find a PDF figure instead.

Also: "Section 3.0" is unusual numbering. Either make it unnumbered or call it 3.1 and shift everything down.

---

## 5. GYO Reduction (Section 3.2) Needs an Example

> "a hyperedge $H$ is an ear if every vertex in $H$ that also appears in some other hyperedge is contained within a single other hyperedge (the *witness*)"

This is a correct definition that a reader needs to re-read twice. For the triangle query you've already introduced, show that GYO reduction *fails* (no ears removable), so the query is cyclic. One concrete example does more than the formal definition alone.

---

## 6. "This is an acceptable tradeoff" is Still There

Line 134:
> "This is an acceptable tradeoff for our setting: we perform hash-based intersection rather than sorted-merge intersection, following Freitag et al.'s insight [5]..."

Just say: "We use hash-based intersection instead of sorted-merge, following Freitag et al. [5]." Don't evaluate your own design choices for the reader.

---

## 7. TRIE-CACHE-GET Pseudocode is Just Memoization

The 4-line pseudocode in Section 5.1 is `if not in cache, build it; return it`. Every reader knows this pattern. The two paragraphs *around* it (explaining identity-based lookup and the two sharing scopes) are the actual content. Cut the pseudocode.

---

## 8. Section 4.3 Row Type Formula is Heavy

```
rowType(Combine) = STRUCT<EXPR$0: ARRAY<tau_0>, ..., EXPR$(N-1): ARRAY<tau_{N-1}>>
```

This is a lot of notation to say "the output is a struct where each field is the result list from one sub-query." The formula duplicates the English sentence right above it. Keep one.

---

## 9. Fingerprinting (5.3.1) Explains Correctness Twice

Paragraph 2: "Two inputs that scan the same table but apply different filters... produce different digests and therefore different fingerprints, correctly preventing prefix sharing when the underlying data differs."

Paragraph 3: "Two fingerprints are equal when they represent the same key intersection over structurally identical inputs... This invariant makes prefix detection correct."

These say the same thing from opposite directions (when fingerprints differ, sharing is prevented; when fingerprints match, sharing is correct). Pick one direction.

---

## 10. TPC-H Query Design Over-Explains Fan-Out

> "Binary joins face intermediate result explosion: the first join on `orderkey` produces $O(|L|^2/|O|)$ tuples (~4x fan-out per row), and the second join on `suppkey` expands further (~600 items per supplier at SF=0.01)."

The SQL example already shows the cycle. The cardinality arithmetic belongs in the *analysis* after the results, not in the query design section. Here it feels like pre-arguing your results before showing them.

---

## 11. "Note that" Pattern

> "Note that `orderkey` appears in two distinct join predicates (L2-L3 and L4-L1), each linking a different pair of lineitem copies (valid because the cycle traverses four copies through four distinct edges)."

"Note that" is a tell for hedging. If it's important, state it directly: "`orderkey` appears in two join predicates (L2-L3 and L4-L1), linking different lineitem copies." If the reader needs this, they need it as a fact, not as an aside.

---

## 12. Future Work is a Wall of Text

The future work paragraph is 130 words in a single run-on block. It already has (1)-(5) markers buried in prose. Make it a real numbered list so it's scannable.

---

## 13. Remaining "Explain Both Sides" Habit

Several spots still explain something abstractly, then give a concrete example in parentheses:

- "frequency analysis to identify structurally identical sub-plans -- branches of the operator tree (e.g., a WCOJ node and its input table scans) that appear in two or more children" (Section 5.2)
- Section 5.1: explains object identity, then explains what it means for self-joins, then explains what it means for cross-operator joins. The two numbered items are clear; the setup paragraph above them is redundant.

Pick the example or the abstraction. Giving both makes sentences long and makes the reader feel talked down to.

---

## Summary Table

| Issue | Section | Fix |
|-------|---------|-----|
| Textbook opening | 1 | Start with the problem, not history |
| Filler contribution | 1 (item 4) | Cut or merge into item 3 |
| Citation dump | 2.3 | Consolidate to 1-2 sentences |
| "Five components" promise | 3.0 | List them or drop the count |
| GYO without example | 3.2 | Show triangle failing GYO |
| "Acceptable tradeoff" | 3.3 | Delete the apology |
| Memoization pseudocode | 5.1 | Cut |
| Row type formula | 4.3 | Keep prose OR formula, not both |
| Double correctness argument | 5.3.1 | Pick one direction |
| Fan-out math in query design | 7.2.4 | Move to analysis or cut |
| "Note that" | 7.2.4 | Rewrite as direct statement |
| Future work wall | 8 | Format as numbered list |
| Explain-then-parenthetical | 5.1, 5.2 | Pick example or abstraction |

These are all small fixes. The structure, results, and technical content are solid.
