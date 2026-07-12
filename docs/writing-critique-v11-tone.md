# Tone Critique: Results Section (Section 7) -- Does This Sound Like an MS Student?

The results section is where the "too polished" problem shows up most. The actual data and analysis are solid, but the *way* things are phrased often reads like someone who's written 10 papers, not their first one. Below I flag specific phrases and suggest alternatives that say the same thing with less polish.

---

## 7.1 Experimental Setup

### Line 361
> "The first four modes span two axes: {binary, WCOJ} x {sequential, batched}. Combine-share extends the batched WCOJ cell with additional sharing rules."

"Span two axes" and "cell" are framework-speak. An MS student would more likely say:

> "The first four modes cover the combinations of join strategy (binary vs. WCOJ) and execution style (one at a time vs. batched). Combine-share adds the sharing rules on top of the batched WCOJ mode."

### Line 363
> "This hub-and-spoke structure mimics the skewed degree distributions found in real-world graphs (social networks, web graphs)."

"Mimics the skewed degree distributions found in" is textbook phrasing. Simpler:

> "This gives the graph a skewed degree distribution, similar to what you see in social networks and web graphs."

### Line 367
> "JVM heap limited to 2 GB (`-Xmx2g`)"

This is fine. Specific and direct. Keep it.

---

## 7.2.1 Scalability with Graph Size

### Line 388
> "WCOJ's advantage grows with graph density (1.5x to 3.2x, Figure 2), as intermediate result explosion in binary joins worsens with graph size while WCOJ tracks output size."

Three things packed into one sentence. "Intermediate result explosion" is a polished compound noun. "Tracks output size" is precise but terse in a way that suggests fluency with the literature. An MS student would probably unpack this:

> "WCOJ gets faster relative to baseline as the graph grows (1.5x at 50 nodes, 3.2x at 400 nodes; Figure 2). This makes sense: bigger graphs mean more intermediate blowup for binary joins, but WCOJ's runtime scales with the actual output size rather than the intermediate."

### Line 388 (continued)
> "Combine adds consistent batching benefit (3.8x at |V|=400), and Combine-Share achieves the peak of **4.0x** once sharing savings outweigh coordination overhead."

"Once sharing savings outweigh coordination overhead" is a clean causal phrase that reads like the author already knows the literature's vocabulary for this tradeoff. Simpler:

> "Combine brings it up to 3.8x at 400 nodes, and Combine-Share gets to **4.0x** at that point -- the sharing is finally saving more work than it costs in overhead."

---

## 7.2.2 Multi-Query Speedup

### Line 406
> "demonstrating sub-linear scaling as fixed costs (plan compilation, trie construction) are amortized across the batch"

"Demonstrating sub-linear scaling as fixed costs are amortized" is textbook performance analysis language. Consider:

> "which shows that the per-query cost goes down as the batch gets bigger, since things like plan compilation and trie construction only happen once"

### Line 407
> "We omit N=2: both batched modes exhibit CV > 50% at that size due to JIT compilation effects."

This sentence is good -- it's honest and specific. The phrasing is fine. Keep it.

### Line 408
> "giving prefix sharing genuine redundancy to eliminate"

Polished. More natural:

> "so prefix sharing actually has duplicate work to cut out"

### Line 408-409
> "At N=20, plain Combine regains the lead (2.7x vs. 2.4x). Section 7.2.3 explains this reversal."

"Explains this reversal" is fine but a bit clinical. A student might write:

> "At N=20, plain Combine is actually faster again (2.7x vs. 2.4x) -- we dig into why in Section 7.2.3."

---

## 7.2.3 Optimization Contributions

### Line 412
> "The four modes form a natural incremental comparison: each mode adds one optimization layer, isolating its marginal contribution."

"Natural incremental comparison" and "isolating its marginal contribution" are economics/systems-paper phrases. Consider:

> "Since each mode adds one more optimization on top of the previous, we can see how much each layer actually helps."

### Line 412 (continued)
> "We report results at two operating points that illustrate different regimes:"

"Operating points" and "regimes" are terms from experienced researchers. Simpler:

> "We picked two configurations that show different behaviors:"

### Line 425-426
> "Combine-Share adds further gains when there is genuine redundancy to eliminate (N=10 has each variation twice, yielding 1.12x over plain Combine). When coordination overhead exceeds sharing savings (e.g., N=20 in Table 3), plain Combine is preferable."

The parenthetical structure and the "when X exceeds Y, Z is preferable" construction are polished. Consider:

> "Combine-Share helps more at N=10 (where each query variation shows up twice, giving it actual duplicates to share), adding 1.12x over plain Combine. But at N=20 the extra overhead from sharing outweighs the savings, and plain Combine does better."

---

## 7.2.4 TPC-H Cyclic Joins

### Line 431
> "To validate these results on realistic data, we construct cyclic join queries over the TPC-H schema"

"To validate these results on realistic data" is a standard transition. Fine, but slightly mechanical. Slightly more natural:

> "The synthetic benchmarks above are all self-joins on a single table, which is the best case for WCOJ. To see how it holds up on more realistic data, we ran cyclic joins over TPC-H"

### Line 467
> "The combine-binary column (1.1x) shows that batching alone contributes little; the gain comes from WCOJ's avoidance of intermediate blowup."

Good analysis, but "the gain comes from WCOJ's avoidance of intermediate blowup" is a very clean causal attribution. More natural:

> "The combine-binary column (1.1x) tells us that batching by itself doesn't help much here -- WCOJ is doing the heavy lifting by avoiding the intermediate blowup."

### Line 480
> "WCOJ pays intersection cost at every level regardless of selectivity, a cost that grows with cycle length: the diamond (5 tables, 4 intersection levels) regresses 3.9x while the rectangle (4 tables, 3 levels) regresses only 1.5x."

This is an excellent sentence of analysis, but the colon-separated structure with parallel phrasing ("the diamond... regresses 3.9x while the rectangle... regresses only 1.5x") reads like someone who's very comfortable with technical writing. Consider breaking it up:

> "WCOJ pays intersection cost at every level no matter how selective the predicate is. The longer the cycle, the more this hurts: the diamond has 5 tables and 4 intersection levels and is 3.9x slower, while the rectangle has 4 tables and 3 levels and is only 1.5x slower."

### Line 480 (continued)
> "This is the $C_{\text{search}}$ gap from Section 3.5."

This callback is good and appropriate. Keep it.

### Line 482
> "with high variance (CV > 20%) suggesting GC pressure from materializing intermediate results"

"Suggesting GC pressure" is a confident hypothesis phrased the way an experienced systems person would. More tentative (and more honest for an MS student):

> "with high variance (CV > 20%), probably because of GC pressure from keeping all those intermediate results in memory"

---

## 7.2.5 Limitations

### Line 486
> "The experiments are subject to several constraints that bound the scope of the claims:"

Classic hedge phrasing. More direct:

> "There are some things our experiments don't cover:"

### Line 488
> "both the WCOJ speedups on self-join queries and the regressions on FK queries are expected to grow, but the crossover point between beneficial and harmful WCOJ application has not been characterized beyond SF=0.01"

"The crossover point between beneficial and harmful WCOJ application has not been characterized" is a very precise way to state a limitation. More natural:

> "we'd expect both the speedups and the slowdowns to get bigger, but we haven't tested where exactly WCOJ stops being worth it at larger scales"

### Line 491
> "The framework's interaction with distributed query processing (e.g., Calcite over Flink or Trino) has not been evaluated."

Passive voice + "interaction with" is formal. Simpler:

> "We haven't tested how this works in a distributed setting (e.g., Calcite running on top of Flink or Trino)."

---

## Section 8 Conclusion

### Line 501
> "Because all contributions are modular extensions that preserve backward compatibility, users can enable WCOJ selectively without disrupting existing workloads."

Reads like a product pitch. More natural:

> "Since everything we added is opt-in and doesn't change existing behavior, users can turn on WCOJ for specific queries without breaking anything."

### Line 503
> "The largest bottleneck in prefix sharing is coordination overhead from materializing intermediate result sets; passing bindings via shared memory could reduce this cost significantly."

This is clean, and the semicolon usage is confident. An MS student might write:

> "The biggest problem with prefix sharing right now is the overhead from materializing all the intermediate results. Using shared memory to pass bindings instead could help a lot."

---

## General Patterns

Across the results section, there are a few recurring habits that collectively make it sound too polished:

1. **Compound causal phrases** like "as X worsens while Y tracks Z" or "once savings outweigh overhead." These are efficient but signal familiarity with writing analysis paragraphs. Break them into two sentences.

2. **Passive constructions for limitations** like "has not been characterized" or "has not been evaluated." These are formally correct but distance the author. Use "we" + active verb: "we haven't tested."

3. **Parallel structure in comparisons**: "the diamond regresses 3.9x while the rectangle regresses only 1.5x." Natural for someone who writes a lot. A first-time author would more likely say each thing separately.

4. **Vocabulary choices**: "operating points," "regimes," "marginal contribution," "amortized," "coordination overhead." All correct, all normal in systems papers, but the density of them signals someone immersed in the literature. Swap a few for plainer words: "configurations" for "operating points," "extra cost" for "coordination overhead."

5. **Clean section transitions**: "To validate these results on realistic data, we construct..." These read like someone who's written many papers with this exact transition structure. Consider starting with the *problem* rather than the *meta-purpose*: "The synthetic benchmarks are all self-joins on one table. TPC-H gives us a chance to test on more realistic data."

Not every instance needs changing -- if you flatten *everything* it'll read unnaturally in the other direction. Pick maybe 60% of these and simplify them. Leave the rest as-is so the paper still reads well.
