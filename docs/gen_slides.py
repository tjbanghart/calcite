#!/usr/bin/env python3
"""Generate presentation slides from the MS Project template."""

from pptx import Presentation
from pptx.util import Inches, Pt
import os

TEMPLATE = "/Users/tjbanghart/Downloads/MS Project.pptx"
OUTPUT = "/Users/tjbanghart/calcite/docs/MS_Project_v2.pptx"
FIG = "/Users/tjbanghart/calcite/docs/figures/"

prs = Presentation(TEMPLATE)

# Layout indices
LY_TITLE = 0
LY_SECTION = 1
LY_TITLE_BODY = 2
LY_TWO_COL = 3
LY_TITLE_ONLY = 4


def get_layout(idx):
    return prs.slide_layouts[idx]


def add_title_body(title, bullets):
    slide = prs.slides.add_slide(get_layout(LY_TITLE_BODY))
    slide.placeholders[0].text = title
    tf = slide.placeholders[1].text_frame
    tf.clear()
    for i, bullet in enumerate(bullets):
        if isinstance(bullet, tuple):
            text, lvl = bullet
        else:
            text, lvl = bullet, 0
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = text
        p.level = lvl
        p.font.size = Pt(14) if lvl == 0 else Pt(12)
    return slide


def add_title_image(title, img_path, img_width=None):
    slide = prs.slides.add_slide(get_layout(LY_TITLE_ONLY))
    slide.placeholders[0].text = title
    if img_width is None:
        img_width = Inches(8.5)
    from PIL import Image as PILImage
    im = PILImage.open(img_path)
    aspect = im.height / im.width
    img_height = int(img_width * aspect)
    left = (prs.slide_width - img_width) // 2
    top = Inches(1.5)
    slide.shapes.add_picture(img_path, left, top, img_width, img_height)
    return slide


def add_section(title):
    slide = prs.slides.add_slide(get_layout(LY_SECTION))
    slide.placeholders[0].text = title
    return slide


def add_image_with_bullets(title, img_path, bullets, img_width=None):
    """Slide with image on the left, bullets on the right."""
    from pptx.util import Emu
    from PIL import Image as PILImage
    slide = prs.slides.add_slide(get_layout(LY_TITLE_ONLY))
    slide.placeholders[0].text = title

    if img_width is None:
        img_width = Inches(5.5)
    im = PILImage.open(img_path)
    aspect = im.height / im.width
    img_height = int(img_width * aspect)
    left = Inches(0.3)
    top = Inches(1.5)
    slide.shapes.add_picture(img_path, left, top, img_width, img_height)

    # Add text box on the right
    from pptx.util import Inches as In
    txBox = slide.shapes.add_textbox(Inches(6.0), Inches(1.5), Inches(3.8), Inches(4.0))
    tf = txBox.text_frame
    tf.word_wrap = True
    for i, bullet in enumerate(bullets):
        if isinstance(bullet, tuple):
            text, lvl = bullet
        else:
            text, lvl = bullet, 0
        p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
        p.text = text
        p.level = lvl
        p.font.size = Pt(12) if lvl == 0 else Pt(10)
    return slide


def add_two_col(title, left_bullets, right_bullets):
    slide = prs.slides.add_slide(get_layout(LY_TWO_COL))
    slide.placeholders[0].text = title
    for ph_idx, bullets in [(1, left_bullets), (2, right_bullets)]:
        tf = slide.placeholders[ph_idx].text_frame
        tf.clear()
        for i, bullet in enumerate(bullets):
            if isinstance(bullet, tuple):
                text, lvl = bullet
            else:
                text, lvl = bullet, 0
            p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
            p.text = text
            p.level = lvl
            p.font.size = Pt(13) if lvl == 0 else Pt(11)
    return slide


# =====================================================================
# Populate existing Motivation slide (slide index 1)
# =====================================================================
mot_slide = prs.slides[1]
tf = mot_slide.placeholders[1].text_frame
tf.clear()
bullets = [
    "How do we optimize a single conjunctive query?",
    ("Join ordering, access path selection, predicate pushdown", 1),
    "What about optimizing multiple queries at a time?",
    ("Shared sub-expressions, common intermediate results", 1),
    ("This is generally known as the view selection problem (VSP)", 1),
    "VSP is NP-Hard -- many heuristics, many real-world systems",
    ("Not a new problem: Finkelstein (1982), Sellis (1988)", 1),
    ("Shortly after System R optimizer, people started thinking about this", 1),
    ("Modern systems: SparkCruise, Amazon Athena fusion, QPipe", 1),
]
for i, bullet in enumerate(bullets):
    if isinstance(bullet, tuple):
        text, lvl = bullet
    else:
        text, lvl = bullet, 0
    p = tf.paragraphs[0] if i == 0 else tf.add_paragraph()
    p.text = text
    p.level = lvl
    p.font.size = Pt(16) if lvl == 0 else Pt(14)


# =====================================================================
# New slides
# =====================================================================

# --- Problem Statement ---
add_title_body("Problem Statement", [
    "Not much has been done to cross MQO with worst-case optimal joins",
    ("WCOJ creates new structures to share -- not just the classic VSP", 1),
    ("Hash tries, search-space traversals, variable-prefix bindings", 1),
    "This is a systems problem, not just an optimization problem",
    ("How do you construct and reliably share data structures at runtime?", 1),
    ("What would it look like integrated into a real query engine?", 1),
    "Open questions:",
    ("How would one implement this in an existing optimizer?", 1),
    ("Is there a theoretical backing for cost estimates?", 1),
    ("When does sharing actually help vs. hurt?", 1),
])

# --- Calcite intro (slide 2 already exists) ---
# The existing Calcite overview slide is at index 2, keep it.

# --- MULTI() Syntax & Combine ---
add_title_body("MULTI() Syntax & the Combine Operator", [
    "New SQL syntax: MULTI((query1), (query2), ...)",
    ("Parser produces a SqlCall with SqlKind.MULTI", 1),
    ("Each sub-query is independently converted to a RelNode", 1),
    "The Combine operator: a trivial way to hold multiple RelNodes",
    ("Single output row = struct of per-query result lists", 1),
    ("Optimizer sees all queries simultaneously", 1),
    ("Sub-queries can use WCOJ, binary joins, or anything else", 1),
    "This work is actually in Calcite today",
    ("Modular, opt-in -- doesn't change existing behavior", 1),
])

# --- MULTI -> Calcite plan example ---
add_two_col("MULTI(): SQL to Calcite Plan", [
    "SQL:",
    ("MULTI(", 1),
    ("  (SELECT e1.src, e1.dst, e2.dst", 1),
    ("   FROM edges e1, e2, e3", 1),
    ("   WHERE e1.dst=e2.src", 1),
    ("   AND e2.dst=e3.src", 1),
    ("   AND e3.dst=e1.src),", 1),
    ("  (SELECT e1.src, e2.src, e3.src", 1),
    ("   FROM edges e1, e2, e3", 1),
    ("   WHERE ...same predicates...)", 1),
    (")", 1),
], [
    "Calcite plan (after WCOJ rule):",
    ("EnumerableCombine", 1),
    ("  EnumerableCalc(proj=[src,dst,dst])", 1),
    ("    EnumerableWCOJ(variables=", 1),
    ("      [Var0[(0,1),(2,1)],", 1),
    ("       Var1[(0,0),(1,0)],", 1),
    ("       Var2[(1,1),(2,0)]])", 1),
    ("      EnumerableTableScan([edges])", 1),
    ("      EnumerableTableScan([edges])", 1),
    ("      EnumerableTableScan([edges])", 1),
    ("  EnumerableCalc(proj=[src,src,src])", 1),
    ("    EnumerableWCOJ(...same...)", 1),
    "",
    "Same WCOJ, different projections",
    ("=> trie cache, spool, prefix share", 1),
])

# --- Architecture ---
arch_png = FIG + "architecture.png"
if os.path.exists(arch_png):
    add_title_image("System Architecture", arch_png, Inches(7))

# --- WCOJ: 3 Ways to Share ---
add_section("WCOJ: Three Ways to Share")

add_title_image("1. Cached Hash Tries",
                FIG + "sharing_trie_cache.png", Inches(9))

add_title_image("2. Sub-Expression Sharing via Spools",
                FIG + "sharing_spooling.png", Inches(9))

add_title_image("3. Shared-Prefix Execution",
                FIG + "sharing_prefix.png", Inches(9))

# --- Results ---
add_section("Experimental Results")

# Test datasets overview
add_two_col("What We Tested", [
    "Synthetic graphs (best case for WCOJ)",
    ("Random directed graphs with a few popular hub nodes", 1),
    ("Hub nodes have lots of connections, most nodes don't", 1),
    ("Like a social network: a few celebrities, many regular users", 1),
    ("Query: find all triangles (A knows B, B knows C, C knows A)", 1),
    ("Binary joins blow up because hubs create huge intermediates", 1),
    ("Run the same triangle query 5-20 times with different output columns", 1),
], [
    "TPC-H (realistic data, ~60 MB)",
    ("Self-join: take the lineitem table (order line items)", 1),
    ("  and join it against itself on different columns", 1),
    ("  \"find items in the same order from the same supplier", 1),
    ("   for the same part\" -- 2.9M matches", 1),
    ("FK joins: chain across real tables", 1),
    ("  customer -> orders -> lineitem -> supplier", 1),
    ("  \"which customers bought from suppliers in the", 1),
    ("   same country?\" -- cycle closes on nationkey", 1),
    ("  Only 25 countries, so binary joins prune fast", 1),
    "",
    "Hardware: M4 Pro, 48 GB, JDK 21, 2 GB heap",
])

add_title_image("Synthetic: Triangle Query on a Graph",
                FIG + "query_synthetic.png", Inches(8))

add_title_image("TPC-H: Self-Join Triangle",
                FIG + "query_selfjoin.png", Inches(8))

add_title_image("TPC-H: FK Cycle Across Tables",
                FIG + "query_fk_cycle.png", Inches(8))

# Explain the modes before showing results
add_title_body("Benchmark Modes: What Are We Comparing?", [
    "Baseline: standard Calcite with binary hash joins, queries run one at a time",
    ("This is what Calcite does today without our changes", 1),
    "WCOJ: replace binary joins with our WCOJ operator, still one query at a time",
    ("Tests whether WCOJ alone helps, independent of batching", 1),
    "Combine: batch N queries with MULTI(), WCOJ + trie caching",
    ("Tests whether running queries together and sharing tries helps", 1),
    "Combine-Share: same as Combine + sub-expression spooling + prefix sharing",
    ("Tests whether the full sharing machinery adds value", 1),
    "Combine-Binary: batch N queries with MULTI() but use binary joins (no WCOJ)",
    ("Isolates the contribution of batching from WCOJ -- TPC-H only", 1),
])

add_image_with_bullets("Scalability with Graph Size", FIG + "scalability.png", [
    "Test: how does performance change as",
    "the graph gets bigger?",
    ("Fix batch at N=5 triangle queries", 1),
    ("Grow graph: 50 to 400 nodes", 1),
    "",
    "Result:",
    ("Baseline grows super-linearly", 1),
    ("  (bigger graph = worse intermediate blowup)", 1),
    ("WCOJ stays flat -- scales with output", 1),
    ("Speedup widens: 1.5x to 4.0x", 1),
])

add_image_with_bullets("Multi-Query Speedup", FIG + "batch_total.png", [
    "Test: does batching more queries",
    "together help?",
    ("Fix graph at 200 nodes, 2000 edges", 1),
    ("Grow batch: N=5 to N=20 queries", 1),
    "",
    "Result:",
    ("Baseline scales linearly (more queries = more time)", 1),
    ("Combine/C-Share scale sub-linearly", 1),
    ("C-Share best at N=10 (3.4x) where", 1),
    ("  each query appears twice (real duplicates)", 1),
    ("At N=20, sharing overhead > savings", 1),
])

add_image_with_bullets("Per-Query Amortized Cost", FIG + "batch_perquery.png", [
    "Test: what does each additional query",
    "cost in a batch?",
    ("Same setup, but divide total time by N", 1),
    "",
    "Result:",
    ("22 ms/query at N=5", 1),
    ("11 ms/query at N=20", 1),
    ("Fixed costs get amortized:", 1),
    ("  plan compilation: once per batch", 1),
    ("  trie construction: once per input", 1),
    ("Adding queries gets cheaper", 1),
])

add_image_with_bullets("Optimization Contributions", FIG + "contributions.png", [
    "Test: how much does each optimization",
    "layer add?",
    ("Each mode builds on the previous one", 1),
    ("Two configurations to show different regimes", 1),
    "",
    "Result:",
    ("WCOJ is the big win (3.2x at V=400)", 1),
    ("Combine adds +0.6x from batching", 1),
    ("C-Share adds +0.2x from sharing", 1),
    ("WCOJ does the heavy lifting;", 1),
    ("  Combine/C-Share are incremental gains", 1),
])

# TPC-H
add_title_body("TPC-H Cyclic Joins", [
    "Standard TPC-H queries are acyclic, but the schema supports cycles",
    ("lineitem self-joins on orderkey/suppkey/partkey", 1),
    ("FK cycles: customer-orders-lineitem-supplier closed by nationkey", 1),
    "Self-join triangle (2.9M rows):",
    ("WCOJ: 2267 ms (1.8x over baseline 4023 ms)", 1),
    ("Combine: 2120 ms (1.9x) -- best mode", 1),
    "Self-join 4-cycle (6.0M rows):",
    ("WCOJ: 46,529 ms (2.4x over baseline 109,443 ms)", 1),
    ("Binary joins OOM on this query; WCOJ completes in 47s", 1),
])

add_image_with_bullets("TPC-H Self-Join Performance", FIG + "tpch_selfjoin.png", [
    "Test: does WCOJ help on realistic",
    "self-join cycles over TPC-H lineitem?",
    ("Same table joined on different columns", 1),
    ("C-Binary added to isolate batching vs WCOJ", 1),
    "",
    "Result:",
    ("Triangle: Combine 1.9x, WCOJ 1.8x", 1),
    ("  C-Binary only 1.1x -- batching alone", 1),
    ("  barely helps; WCOJ does the real work", 1),
    ("4-Cycle: WCOJ 2.4x (46.5s vs 109s)", 1),
    ("  Binary joins OOM at 4 GB heap", 1),
    ("  WCOJ enables queries binary can't run", 1),
])

add_title_body("TPC-H: FK Cycles (WCOJ Loses)", [
    "FK rectangle (c-o-l-s, 11.7K rows): WCOJ 1.5x slower",
    ("Baseline 902 ms vs WCOJ 1381 ms", 1),
    "FK diamond (c-o-l-s-n, 11.7K rows): WCOJ 3.9x slower",
    ("Baseline 2096 ms vs WCOJ 8141 ms", 1),
    "Why? Low-cardinality closing predicate (nationkey, 25 values)",
    ("Binary joins prune early via selective FK constraints", 1),
    ("WCOJ pays intersection cost at every level regardless", 1),
    "Combine makes it worse on the diamond (15.8s)",
    ("Materialization overhead on top of already-slow WCOJ", 1),
])

add_image_with_bullets("TPC-H FK Cycle Performance", FIG + "tpch_fk.png", [
    "Test: what happens when the cycle",
    "crosses different tables via FK?",
    ("customer -> orders -> lineitem -> supplier", 1),
    ("Closed by nationkey (only 25 values)", 1),
    "",
    "Result: WCOJ loses",
    ("Rectangle: 1.5x slower (1381 vs 902 ms)", 1),
    ("Diamond: 3.9x slower (8.1s vs 2.1s)", 1),
    ("Combine makes it even worse (15.8s)", 1),
    "",
    "Why?",
    ("Binary joins prune early on selective FK", 1),
    ("WCOJ pays intersection cost at every level", 1),
    ("Longer cycle = more wasted intersection", 1),
])

# --- Summary ---
add_section("Summary")

add_title_body("Contributions to Calcite", [
    "Multi-query infrastructure (in Calcite today):",
    ("Combine operator: holds N sub-queries as one plan", 1),
    ("MULTI() SQL syntax: declares a batch for joint optimization", 1),
    ("Sub-expression spooling: materializes shared sub-plans once", 1),
    "Single-query WCOJ algorithm:",
    ("Hash-based worst-case optimal join operator", 1),
    ("GYO cyclicity detection -- only fires on genuine cycles", 1),
    ("Integrated into Volcano optimizer via planner rules", 1),
    "All modular, opt-in, backward compatible",
    ("Existing queries are unaffected", 1),
])

add_title_body("Lessons Learned & Open Problems", [
    "Cyclic != use WCOJ",
    ("Need a way to identify cycles where binary joins still win", 1),
    ("Selective closing predicates (e.g., nationkey) let binary joins prune early", 1),
    ("Freitag et al. saw the same in Umbra -- cost-based routing is the real answer", 1),
    "Calcite's execution engine is the wrong place to benchmark",
    ("Most systems use Calcite for planning, not execution", 1),
    ("Better case: export plans to Flink, Trino, or another engine", 1),
    ("Could also work for streaming queries or materializing frequently-used structures", 1),
    "MULTI() requires knowing the workload ahead of time",
    ("You have to declare the batch up front", 1),
    ("This is still the candidate selection problem -- NP-hard in general", 1),
    ("A real system would need workload analysis or online detection", 1),
])

# --- Thank you ---
# Slide already exists at the end, keep it.

# =====================================================================
# Save
# =====================================================================
prs.save(OUTPUT)
print(f"Saved to {OUTPUT}")
print(f"Total slides: {len(prs.slides)}")
