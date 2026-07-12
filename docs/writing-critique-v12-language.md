# Language Complexity Critique -- Full Paper, Focus on Results

The concern: this reads like someone who has published before. The analysis is good, the data is good, but the *sentences* are too practiced. Below I go through the paper with concrete rewrites. I'm not suggesting you take every single one -- that would overcorrect. But hitting maybe half of these would make the voice feel more like a real first paper.

I'm grouping by what makes each phrase sound too polished, not by section order.

---

## Pattern 1: Compound causal sentences

These pack two or three ideas into one sentence using subordinate clauses, semicolons, and appositives. They're efficient but they signal someone who's comfortable compressing analysis. A first-time author usually says one thing per sentence.

**Line 16** (Intro):
> "The resulting runtime is bounded by the *AGM bound* [2], the information-theoretic maximum output size given the input cardinalities, rather than by intermediate result sizes."

The appositive definition is slick. Break it:

> "The resulting runtime is bounded by the *AGM bound* [2] rather than by intermediate result sizes. The AGM bound gives the information-theoretic maximum output size given the input cardinalities."

**Line 388** (Results 7.2.1):
> "WCOJ's advantage grows with graph density (1.5x to 3.2x, Figure 2), as intermediate result explosion in binary joins worsens with graph size while WCOJ tracks output size."

Three ideas in one sentence ("advantage grows", "explosion worsens", "WCOJ tracks"). Split:

> "WCOJ gets faster relative to baseline as the graph grows, going from 1.5x at 50 nodes to 3.2x at 400 (Figure 2). Bigger graphs mean more intermediate blowup for binary joins, but WCOJ scales with the actual output size."

**Line 388 (cont.)**:
> "Combine adds consistent batching benefit (3.8x at |V|=400), and Combine-Share achieves the peak of 4.0x once sharing savings outweigh coordination overhead."

> "Combine brings it up to 3.8x at 400 nodes. Combine-Share reaches **4.0x** at that size -- the sharing is saving more work than it costs in overhead."

**Line 480** (Results 7.2.4):
> "WCOJ pays intersection cost at every level regardless of selectivity, a cost that grows with cycle length: the diamond (5 tables, 4 intersection levels) regresses 3.9x while the rectangle (4 tables, 3 levels) regresses only 1.5x."

Parallel structure with colon-separated explanation:

> "WCOJ pays intersection cost at every level no matter how selective the predicate is, and the longer the cycle, the worse this gets. The diamond (5 tables, 4 intersection levels) is 3.9x slower, while the rectangle (4 tables, 3 levels) is only 1.5x slower."

**Line 158** (Cost model):
> "When $C_{\text{search}}$ is small relative to $C_{\text{build}} + C_{\text{enum}}$ (self-join cycles with large output), it approximates the true cost well. When $C_{\text{search}}$ dominates (FK cycles where a variable has high fan-out but low final selectivity), WCOJ can be slower than binary joins."

The mirrored "When X... When Y..." structure is a classic rhetorical parallel. This one is fine for a formal section, but the parentheticals are doing a lot of work. Consider:

> "For self-join cycles with large output, $C_{\text{search}}$ is small compared to $C_{\text{build}} + C_{\text{enum}}$, and the lower bound approximates the true cost well. But for FK cycles where a variable has high fan-out but low final selectivity, $C_{\text{search}}$ can dominate, and WCOJ ends up slower than binary joins."

---

## Pattern 2: Experienced-researcher vocabulary

Individual words that are fine on their own but collectively signal deep fluency with systems-paper conventions.

**Line 412** (Results 7.2.3):
> "We report results at two **operating points** that illustrate different **regimes**:"

→ "We show results at two different configurations:"

**Line 412**:
> "isolating its **marginal contribution**"

→ "so we can see how much each one actually helps"

**Line 406** (Results 7.2.2):
> "**demonstrating sub-linear scaling** as fixed costs (plan compilation, trie construction) **are amortized** across the batch"

→ "which shows the per-query cost goes down as the batch gets bigger, since things like plan compilation and trie building only happen once"

**Line 363**:
> "This hub-and-spoke structure **mimics the skewed degree distributions found in** real-world graphs"

→ "This gives the graph a skewed degree distribution, like you'd see in social networks or web graphs"

**Line 205**:
> "The `TrieCache` creation in step 1 is the **critical bridge** for cross-query optimization"

→ "The `TrieCache` created in step 1 is what makes cross-query optimization possible"

**Line 40**:
> "Their approach is the **closest precursor** to ours"

→ "Their approach is the most similar to ours"

**Line 57**:
> "Our `Combine` operator **draws on this line of work** but **composes with** WCOJ"

→ "Our `Combine` operator is inspired by this work but works with WCOJ instead of binary joins"

---

## Pattern 3: Formulaic academic transitions

Phrases that are structurally correct but feel like fill-in-the-blank templates.

**Line 431** (Results 7.2.4):
> "To validate these results on realistic data, we construct cyclic join queries over the TPC-H schema"

The "To validate X, we Y" pattern is a standard transition. More natural to start with the motivation:

> "The synthetic benchmarks above are all self-joins on one table, which is the best case for WCOJ. To see how things look on more realistic data, we tried cyclic joins over TPC-H"

**Line 412** (Results 7.2.3):
> "The four modes form a **natural incremental comparison**: each mode adds one optimization layer"

> "Since each mode adds one optimization on top of the previous one, we can break down how much each layer helps"

**Line 486** (Limitations):
> "The experiments are **subject to several constraints that bound the scope of** the claims:"

> "There are some things our experiments don't cover:"

**Line 20** (Intro):
> "In this paper, we present an integrated system within Apache Calcite [9] that combines WCOJ execution with multi-query optimization. Our contributions are:"

"In this paper, we present" is the most standard opening imaginable. It's fine, everyone uses it. But if you want to sound less practiced, just drop the meta-framing:

> "We built a system within Apache Calcite [9] that combines WCOJ with multi-query optimization. The main contributions are:"

---

## Pattern 4: Overly clean parallel constructions

**Line 18** (Intro):
> "trie indices can be shared across queries, identical search trees can be traversed once, and shared variable prefixes can be computed once and distributed to per-query suffix executors"

Three parallel passive clauses. A first-time author would probably not maintain such clean parallelism:

> "tries can be reused across queries, identical parts of the search don't need to be traversed more than once, and when queries share the same variable prefix, that prefix can be computed once and handed off to per-query suffix executors"

**Line 211** (Section 5):
> "trie caching shares *data structures* (the hash tries built from input relations), sub-expression sharing shares *sub-plans* (entire branches of the relational operator tree...), and prefix sharing shares *computation* (the backtracking search over shared join variables)."

Clean taxonomy with parallel parentheticals. This is good *content* but the *form* is very practiced. Consider keeping the table (which already does this) and simplifying the prose:

> "These target different kinds of redundancy. Trie caching avoids rebuilding the same hash tries. Sub-expression sharing avoids running the same operator subtree multiple times. Prefix sharing avoids traversing the same WCOJ search space twice."

---

## Pattern 5: Confident hypothesizing

An MS student would typically hedge more when explaining unexpected results.

**Line 482**:
> "with high variance (CV > 20%) **suggesting GC pressure** from materializing intermediate results"

→ "with high variance (CV > 20%), which we think is probably GC pressure from keeping all those intermediate results in memory"

**Line 408**:
> "giving prefix sharing **genuine redundancy to eliminate**"

→ "so prefix sharing actually has duplicate work it can cut"

**Line 467**:
> "the gain comes from WCOJ's avoidance of intermediate blowup"

→ "it's WCOJ doing the real work by avoiding the intermediate blowup"

---

## Pattern 6: Repeating the thesis too cleanly

"Combine is agnostic to join strategy" appears in some form at lines 28, 166, and 499. Once is fine. Three times with slightly different wording sounds like a rehearsed talking point. Pick the best one (probably line 28 since it's the intro) and cut or rephrase the others.

---

## What already sounds natural

To be clear, a lot of this paper already reads well for an MS student:

- The pseudocode sections (3.4) are clear and not over-explained
- The data tables are well-formatted and honest (marking CV>20%, noting OOM)
- The "We omit N=2" honesty (line 407) is exactly what a first paper should do
- The GYO reduction walkthrough (line 93-94) explains things clearly without being condescending
- The limitations section (7.2.5) is appropriately candid
- The TPC-H FK cyclicity paragraph is great -- it shows something interesting you found and explains it well

The problem isn't that the paper is *bad*. It's that the *sentences* are too efficiently constructed for someone who hasn't written a paper before. The fix is mostly about breaking compound sentences apart and swapping a handful of vocabulary choices.
