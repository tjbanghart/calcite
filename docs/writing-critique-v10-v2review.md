# Review: wcoj-mqo-calcite-v2.md + TPC-H Tests (Pass 2)

Items from Pass 1 that have been fixed:
- ~~Reference numbering gaps~~ (now 1-22, sequential)
- ~~Section 2.5 title~~ (now "Gap: WCOJ + MQO")
- ~~Table 3 column headers~~ (now "/query (Comb.)" and "/query (C-S)")
- ~~Empirical verification after Theorem 1~~ (line 322 added)
- ~~Abstract comma~~ (added)
- ~~fk-triangle removed from ALL_SHAPES~~ (with explanatory comment)
- ~~verbose default~~ (now false)
- ~~Diamond Javadoc 3.6x~~ (updated to 3.9x)
- ~~4-cycle test comment~~ (now correctly attributes cross-cycles to sk=10 and sk=20)

---

## Paper: Remaining Issues

### 1. The 9.5x claim is unsupported by Table 3

Abstract (line 3) and conclusion (line 502) both say "per-query cost dropping 9.5x as batch size grows." But Table 3 shows per-query Combine cost going from 22ms (N=5) to 11ms (N=20), which is 2x. Even comparing per-query baseline at N=5 (274/5 = 54.8ms) to per-query Combine at N=20 (11ms) gives 5x. I can't find any combination of numbers in the tables that produces 9.5x.

This will get flagged. Either the 9.5x comes from data at a larger N that isn't shown (in which case add the data point), or it's a stale number from a previous run. Verify or replace.

### 2. Broken forward reference: "Section 8, item 2"

Line 147: "adaptive ordering is future work (Section 8, item 2)." But Section 8 (Conclusion) is now a single paragraph of future directions with no numbered items. Either number the future work items or change line 147 to just say "adaptive ordering is future work (Section 8)."

### 3. Redundant sentence about the floor parameter

Lines 294 and 306 say the same thing:
- Line 294: "A floor parameter prevents backtracking below the prefix boundary"
- Line 306: "The `floor` parameter prevents backtracking below the prefix boundary, confining suffix execution to the per-query divergent portion of the search space."

Delete one or combine them. The second adds "confining suffix execution to the per-query divergent portion" which is useful, so consider keeping only that one.

### 4. LaTeX commands in markdown

Lines 428 (`\newpage`) and 468 (`\needspace{8\baselineskip}`) are still present. If this file is meant to be read as markdown at any point (e.g., on GitHub or during review), these will render as literal text. If it's pandoc-only, fine.

### 5. Section 3.5 cost model: "valid" is undefined

Line 157: $\sum_{(v_1,\ldots,v_{k-1}) \in \text{valid}}$ -- "valid" is used as a set name but never formally defined. It implicitly means "all prefix bindings that survived intersection at levels 1 through k-1." A brief parenthetical would help: e.g., "where $\text{valid}$ denotes the set of prefix bindings surviving intersection at prior levels."

### 6. "2x2 decomposition" with 5 modes

Line 362: "These modes form a 2x2 decomposition: {binary, WCOJ} x {sequential, batched}." But there are 5 modes listed (baseline, wcoj, combine, combine-share, combine-binary). Combine-share doesn't fit the 2x2. This isn't wrong exactly (you're describing the conceptual decomposition, and combine-share is an extension), but a reader counting 5 rows in the table will stumble. Consider: "The first four modes form a 2x2 decomposition..." or "These modes span two axes..."

### 7. Tone: a few spots still read like a senior researcher

These are minor and depend on how much you want to soften:

- Line 28: "The value of the `Combine` framework is that it is agnostic to join strategy, letting the optimizer route each sub-query to the best approach." This reads like a polished thesis summary. Slightly more natural: "What makes `Combine` useful is that it doesn't care about join strategy -- the optimizer can route each sub-query to whatever works best."

- Line 504: "For practitioners considering WCOJ, our results suggest..." -- "For practitioners" sounds like you've been writing advice columns for industry. Consider: "Based on our results, WCOJ seems most beneficial when..."

- The contributions list (lines 22-27) is clean. Maybe too clean. If you want MS-student voice, contribution 3 could be less of a polished bullet: "We also explore three cross-query optimizations..." instead of the declarative "Three optimizations that exploit WCOJ's structure."

These are soft suggestions -- the current version is perfectly fine for a thesis. Only change if tone really matters.

### 8. Abstract: "intermediate results larger than the final output"

Line 3: "no join ordering avoids intermediate results larger than the final output." This is slightly imprecise. What you mean is that intermediates can be *asymptotically* larger than the output. For some join orderings, intermediates might be merely slightly larger than the output but still unavoidable. More precise: "no join ordering avoids intermediate results that can be much larger than the final output." But this is nitpicky.

### 9. Missing comma in line 9

"For acyclic queries this works well" should be "For acyclic queries**,** this works well" (introductory clause).

---

## Tests: Remaining Issues

### 10. No dedicated GYO test for the FK-triangle acyclicity claim

Still the main gap. The paper makes a specific structural argument about why lineitem-partsupp-supplier is alpha-acyclic (ternary suppkey hyperedge witnesses binary partkey hyperedge). `testAlphaCyclicityDetection` tests a generic triangle+U case but not this specific pattern. A test case with hyperedges `{0, 1}` (partkey: l, ps) and `{0, 1, 2}` (suppkey: l, ps, s), asserting `isAlphaCyclic == false`, would directly validate the paper's claim.

### 11. SQL-level tests don't guarantee WCOJ fires

The SQL TPC-H tests add WCOJ rules but don't remove binary join rules, so the optimizer may choose binary joins. This is fine for correctness, but if you want to prove WCOJ actually executes these queries, add `.explainContains("EnumerableWCOJ")` (gated on `ENABLE_WCOJ`). Low priority since the runtime-level tests already verify WCOJ correctness directly.

### 12. FK-diamond naming

"Diamond" for a 5-table cycle is still a bit misleading (diamond usually implies 4 nodes). The Javadoc now says "5-cycle" which helps, but someone grepping for the name might be confused. Consider renaming to `fk-5cycle` or just living with it since it's consistent with the paper's usage.

---

## Summary (Pass 2)

The highest-priority item is **#1 (the 9.5x claim)**. If that number can't be derived from the data in the paper, a reviewer will catch it immediately. Everything else is minor polish:
- Fix the broken "Section 8, item 2" forward reference (#2)
- Remove the redundant floor-parameter sentence (#3)
- Add a GYO test for the FK-triangle claim (#10)
- Consider defining "valid" in the cost model (#5)
- Add comma after "For acyclic queries" (#9)
