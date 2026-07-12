#!/usr/bin/env python3
"""Generate visual diagrams for the three WCOJ sharing optimizations."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

C_GREY = "#D4D4D4"
C_GREY_EC = "#999999"
C_BLUE = "#A8C8E8"
C_BLUE_EC = "#4477AA"
C_ORANGE = "#FFCC88"
C_ORANGE_EC = "#EE7733"
C_GREEN = "#B8DDB8"
C_GREEN_EC = "#228833"
C_RED = "#FFAAAA"
C_RED_EC = "#CC4444"
C_ARROW = "#555555"

OUT = "figures/"


def box(ax, x, y, w, h, label, fc, ec, fontsize=10, bold=False, sublabel=None):
    rect = mpatches.FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.06",
        facecolor=fc, edgecolor=ec, linewidth=1.5)
    ax.add_patch(rect)
    weight = "bold" if bold else "normal"
    ty = y + h/2 + (0.08 if sublabel else 0)
    ax.text(x + w/2, ty, label, ha="center", va="center",
            fontsize=fontsize, fontweight=weight)
    if sublabel:
        ax.text(x + w/2, y + h/2 - 0.12, sublabel,
                ha="center", va="center", fontsize=8, fontstyle="italic",
                color="#444444")
    return dict(cx=x+w/2, t=y+h, b=y, l=x, r=x+w)


def arrow(ax, x1, y1, x2, y2, color=C_ARROW, style="-|>", lw=1.2):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle=style, color=color, lw=lw))


def dashed_arrow(ax, x1, y1, x2, y2, color=C_ARROW):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=1.2,
                                linestyle="dashed"))


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT + name + ".png", dpi=600, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    print(f"  {name}.png")


# =========================================================================
# 1. Trie Caching
# =========================================================================
def fig_trie_caching():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

    for ax in [ax1, ax2]:
        ax.set_xlim(0, 6)
        ax.set_ylim(0, 4)
        ax.axis("off")

    # LEFT: Without caching
    ax1.set_title("Without Trie Cache", fontsize=13, fontweight="bold", pad=10)

    w1 = box(ax1, 0.3, 3.0, 2.2, 0.5, "WCOJ (Q1)", C_BLUE, C_BLUE_EC, bold=True)
    w2 = box(ax1, 3.5, 3.0, 2.2, 0.5, "WCOJ (Q2)", C_BLUE, C_BLUE_EC, bold=True)

    t1a = box(ax1, 0.0, 1.8, 1.2, 0.5, "Trie A", C_RED, C_RED_EC, fontsize=9)
    t1b = box(ax1, 1.5, 1.8, 1.2, 0.5, "Trie A'", C_RED, C_RED_EC, fontsize=9)
    t2a = box(ax1, 3.2, 1.8, 1.2, 0.5, "Trie A''", C_RED, C_RED_EC, fontsize=9)
    t2b = box(ax1, 4.7, 1.8, 1.2, 0.5, "Trie A'''", C_RED, C_RED_EC, fontsize=9)

    s1 = box(ax1, 0.7, 0.5, 1.2, 0.5, "Scan(edges)", C_GREY, C_GREY_EC, fontsize=9)
    s2 = box(ax1, 4.0, 0.5, 1.2, 0.5, "Scan(edges)", C_GREY, C_GREY_EC, fontsize=9)

    arrow(ax1, w1["cx"]-0.4, w1["b"], t1a["cx"], t1a["t"])
    arrow(ax1, w1["cx"]+0.4, w1["b"], t1b["cx"], t1b["t"])
    arrow(ax1, w2["cx"]-0.4, w2["b"], t2a["cx"], t2a["t"])
    arrow(ax1, w2["cx"]+0.4, w2["b"], t2b["cx"], t2b["t"])
    arrow(ax1, t1a["cx"], t1a["b"], s1["cx"], s1["t"])
    arrow(ax1, t1b["cx"], t1b["b"], s1["cx"], s1["t"])
    arrow(ax1, t2a["cx"], t2a["b"], s2["cx"], s2["t"])
    arrow(ax1, t2b["cx"], t2b["b"], s2["cx"], s2["t"])

    ax1.text(3.0, 0.1, "4 tries built from same data", ha="center",
             fontsize=11, color=C_RED_EC, fontweight="bold")

    # RIGHT: With caching
    ax2.set_title("With Trie Cache", fontsize=13, fontweight="bold", pad=10)

    w1 = box(ax2, 0.3, 3.0, 2.2, 0.5, "WCOJ (Q1)", C_BLUE, C_BLUE_EC, bold=True)
    w2 = box(ax2, 3.5, 3.0, 2.2, 0.5, "WCOJ (Q2)", C_BLUE, C_BLUE_EC, bold=True)

    cache = box(ax2, 1.8, 1.8, 2.4, 0.5, "TrieCache: 1 trie", C_GREEN, C_GREEN_EC,
                bold=True, fontsize=10)

    s1 = box(ax2, 2.3, 0.5, 1.4, 0.5, "Scan(edges)", C_GREY, C_GREY_EC, fontsize=9)

    arrow(ax2, w1["cx"], w1["b"], cache["cx"]-0.4, cache["t"])
    arrow(ax2, w2["cx"], w2["b"], cache["cx"]+0.4, cache["t"])
    arrow(ax2, cache["cx"], cache["b"], s1["cx"], s1["t"])

    ax2.text(3.0, 0.1, "1 trie built, shared via cache", ha="center",
             fontsize=11, color=C_GREEN_EC, fontweight="bold")

    save(fig, "sharing_trie_cache")


# =========================================================================
# 2. Sub-Expression Sharing (Spooling)
# =========================================================================
def fig_spooling():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))

    for ax in [ax1, ax2]:
        ax.set_xlim(0, 6)
        ax.set_ylim(0, 4.5)
        ax.axis("off")

    # LEFT: Before
    ax1.set_title("Before (duplicate sub-plans)", fontsize=13, fontweight="bold", pad=10)

    c = box(ax1, 1.8, 3.8, 2.4, 0.45, "Combine", C_GREY, C_GREY_EC, bold=True)
    p1 = box(ax1, 0.2, 3.0, 1.8, 0.45, "Project(a,b,c)", C_GREY, C_GREY_EC, fontsize=9)
    p2 = box(ax1, 4.0, 3.0, 1.8, 0.45, "Project(x,y,z)", C_GREY, C_GREY_EC, fontsize=9)
    w1 = box(ax1, 0.2, 2.1, 1.8, 0.45, "WCOJ(...)", C_RED, C_RED_EC, fontsize=9, bold=True)
    w2 = box(ax1, 4.0, 2.1, 1.8, 0.45, "WCOJ(...)", C_RED, C_RED_EC, fontsize=9, bold=True)
    s1a = box(ax1, 0.0, 1.2, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)
    s1b = box(ax1, 1.1, 1.2, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)
    s2a = box(ax1, 3.8, 1.2, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)
    s2b = box(ax1, 4.9, 1.2, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)

    arrow(ax1, c["cx"]-0.5, c["b"], p1["cx"], p1["t"])
    arrow(ax1, c["cx"]+0.5, c["b"], p2["cx"], p2["t"])
    arrow(ax1, p1["cx"], p1["b"], w1["cx"], w1["t"])
    arrow(ax1, p2["cx"], p2["b"], w2["cx"], w2["t"])
    arrow(ax1, w1["cx"]-0.3, w1["b"], s1a["cx"], s1a["t"])
    arrow(ax1, w1["cx"]+0.3, w1["b"], s1b["cx"], s1b["t"])
    arrow(ax1, w2["cx"]-0.3, w2["b"], s2a["cx"], s2a["t"])
    arrow(ax1, w2["cx"]+0.3, w2["b"], s2b["cx"], s2b["t"])

    ax1.text(3.0, 0.7, "Identical WCOJ computed twice", ha="center",
             fontsize=11, color=C_RED_EC, fontweight="bold")

    # RIGHT: After
    ax2.set_title("After (spool + spool read)", fontsize=13, fontweight="bold", pad=10)

    c = box(ax2, 1.8, 3.8, 2.4, 0.45, "Combine", C_GREY, C_GREY_EC, bold=True)
    p1 = box(ax2, 0.2, 3.0, 1.8, 0.45, "Project(a,b,c)", C_GREY, C_GREY_EC, fontsize=9)
    p2 = box(ax2, 4.0, 3.0, 1.8, 0.45, "Project(x,y,z)", C_GREY, C_GREY_EC, fontsize=9)
    sp = box(ax2, 0.2, 2.1, 1.8, 0.45, "SPOOL", C_GREEN, C_GREEN_EC, fontsize=9, bold=True)
    rd = box(ax2, 4.0, 2.1, 1.8, 0.45, "READ_SPOOL", C_GREEN, C_GREEN_EC, fontsize=9, bold=True)
    w1 = box(ax2, 0.2, 1.2, 1.8, 0.45, "WCOJ(...)", C_BLUE, C_BLUE_EC, fontsize=9)
    s1a = box(ax2, 0.0, 0.4, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)
    s1b = box(ax2, 1.1, 0.4, 0.9, 0.4, "Scan", C_GREY, C_GREY_EC, fontsize=8)

    arrow(ax2, c["cx"]-0.5, c["b"], p1["cx"], p1["t"])
    arrow(ax2, c["cx"]+0.5, c["b"], p2["cx"], p2["t"])
    arrow(ax2, p1["cx"], p1["b"], sp["cx"], sp["t"])
    arrow(ax2, p2["cx"], p2["b"], rd["cx"], rd["t"])
    arrow(ax2, sp["cx"], sp["b"], w1["cx"], w1["t"])
    dashed_arrow(ax2, sp["r"]+0.1, sp["b"]+0.22, rd["l"]-0.1, rd["b"]+0.22,
                 color=C_GREEN_EC)
    arrow(ax2, w1["cx"]-0.3, w1["b"], s1a["cx"], s1a["t"])
    arrow(ax2, w1["cx"]+0.3, w1["b"], s1b["cx"], s1b["t"])

    ax2.text(3.0, 0.0, "WCOJ runs once, results materialized & reused", ha="center",
             fontsize=11, color=C_GREEN_EC, fontweight="bold")

    save(fig, "sharing_spooling")


# =========================================================================
# 3. Shared-Prefix Execution (partial overlap example)
# =========================================================================
def fig_prefix_sharing():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6.5))

    for ax in [ax1, ax2]:
        ax.set_xlim(0, 7)
        ax.set_ylim(0, 5.5)
        ax.axis("off")

    # --- LEFT: Without prefix sharing ---
    ax1.set_title("Without Prefix Sharing", fontsize=13, fontweight="bold", pad=10)

    # Q1: joins on (orderkey, suppkey, partkey)
    ax1.text(1.5, 5.2, "Q1: join on ok, sk, pk", fontsize=10, ha="center",
             fontweight="bold", color=C_BLUE_EC)
    r1 = box(ax1, 0.7, 4.4, 1.6, 0.4, "ok=1", C_RED, C_RED_EC, fontsize=9)
    s1a = box(ax1, 0.1, 3.5, 1.1, 0.4, "sk=10", C_RED, C_RED_EC, fontsize=9)
    s1b = box(ax1, 1.5, 3.5, 1.1, 0.4, "sk=20", C_RED, C_RED_EC, fontsize=9)
    p1a = box(ax1, 0.1, 2.6, 1.1, 0.4, "pk=100", C_ORANGE, C_ORANGE_EC, fontsize=9)
    p1b = box(ax1, 1.5, 2.6, 1.1, 0.4, "pk=200", C_ORANGE, C_ORANGE_EC, fontsize=9)
    arrow(ax1, r1["cx"]-0.3, r1["b"], s1a["cx"], s1a["t"])
    arrow(ax1, r1["cx"]+0.3, r1["b"], s1b["cx"], s1b["t"])
    arrow(ax1, s1a["cx"], s1a["b"], p1a["cx"], p1a["t"])
    arrow(ax1, s1b["cx"], s1b["b"], p1b["cx"], p1b["t"])

    # Q2: joins on (orderkey, suppkey, discount) -- different 3rd variable
    ax1.text(5.5, 5.2, "Q2: join on ok, sk, disc", fontsize=10, ha="center",
             fontweight="bold", color=C_BLUE_EC)
    r2 = box(ax1, 4.7, 4.4, 1.6, 0.4, "ok=1", C_RED, C_RED_EC, fontsize=9)
    s2a = box(ax1, 4.1, 3.5, 1.1, 0.4, "sk=10", C_RED, C_RED_EC, fontsize=9)
    s2b = box(ax1, 5.5, 3.5, 1.1, 0.4, "sk=20", C_RED, C_RED_EC, fontsize=9)
    d2a = box(ax1, 4.1, 2.6, 1.1, 0.4, "disc=5%", C_ORANGE, C_ORANGE_EC, fontsize=9)
    d2b = box(ax1, 5.5, 2.6, 1.1, 0.4, "disc=8%", C_ORANGE, C_ORANGE_EC, fontsize=9)
    arrow(ax1, r2["cx"]-0.3, r2["b"], s2a["cx"], s2a["t"])
    arrow(ax1, r2["cx"]+0.3, r2["b"], s2b["cx"], s2b["t"])
    arrow(ax1, s2a["cx"], s2a["b"], d2a["cx"], d2a["t"])
    arrow(ax1, s2b["cx"], s2b["b"], d2b["cx"], d2b["t"])

    # Highlight the duplicated work
    from matplotlib.patches import FancyBboxPatch
    dup1 = FancyBboxPatch((0.0, 3.3), 2.8, 1.7, boxstyle="round,pad=0.1",
                          facecolor="none", edgecolor=C_RED_EC, linewidth=2, linestyle="--")
    dup2 = FancyBboxPatch((4.0, 3.3), 2.8, 1.7, boxstyle="round,pad=0.1",
                          facecolor="none", edgecolor=C_RED_EC, linewidth=2, linestyle="--")
    ax1.add_patch(dup1)
    ax1.add_patch(dup2)
    ax1.text(3.5, 4.1, "same\nwork", fontsize=10, ha="center", va="center",
             color=C_RED_EC, fontweight="bold")

    ax1.text(3.5, 2.0, "ok, sk searched twice\n(identical intersection + backtracking)",
             ha="center", fontsize=10, color=C_RED_EC, fontweight="bold")

    # --- RIGHT: With prefix sharing ---
    ax2.set_title("With Prefix Sharing", fontsize=13, fontweight="bold", pad=10)

    # Shared prefix: orderkey, suppkey
    ax2.text(3.5, 5.2, "Shared Prefix (Phase 1): ok, sk", fontsize=11,
             ha="center", fontweight="bold", color=C_GREEN_EC)
    r = box(ax2, 2.7, 4.4, 1.6, 0.4, "ok=1", C_GREEN, C_GREEN_EC, fontsize=10, bold=True)
    s1 = box(ax2, 1.8, 3.5, 1.2, 0.4, "sk=10", C_GREEN, C_GREEN_EC, fontsize=10, bold=True)
    s2 = box(ax2, 4.0, 3.5, 1.2, 0.4, "sk=20", C_GREEN, C_GREEN_EC, fontsize=10, bold=True)
    arrow(ax2, r["cx"]-0.3, r["b"], s1["cx"], s1["t"])
    arrow(ax2, r["cx"]+0.3, r["b"], s2["cx"], s2["t"])

    # Floor line
    ax2.plot([0.3, 6.7], [3.05, 3.05], color=C_GREEN_EC, linewidth=2, linestyle="--")
    ax2.text(6.5, 3.15, "floor", fontsize=10, color=C_GREEN_EC, fontstyle="italic",
             ha="right")

    # Q1 suffix: partkey
    ax2.text(1.5, 2.7, "Q1 suffix: pk", fontsize=9, ha="center",
             fontweight="bold", color=C_BLUE_EC)
    pk1 = box(ax2, 0.5, 2.0, 1.0, 0.35, "pk=100", C_ORANGE, C_ORANGE_EC, fontsize=8)
    pk2 = box(ax2, 1.7, 2.0, 1.0, 0.35, "pk=200", C_ORANGE, C_ORANGE_EC, fontsize=8)
    arrow(ax2, s1["cx"]-0.1, s1["b"]-0.15, pk1["cx"], pk1["t"], color=C_BLUE_EC)
    arrow(ax2, s1["cx"]+0.4, s1["b"]-0.15, pk2["cx"], pk2["t"], color=C_BLUE_EC)

    res1 = box(ax2, 0.5, 1.3, 2.2, 0.35, "emit (ok, sk, pk) rows", C_GREY, C_GREY_EC,
               fontsize=8)
    arrow(ax2, pk1["cx"]+0.3, pk1["b"], res1["cx"]-0.3, res1["t"])
    arrow(ax2, pk2["cx"]-0.3, pk2["b"], res1["cx"]+0.3, res1["t"])

    # Q2 suffix: discount
    ax2.text(5.5, 2.7, "Q2 suffix: disc", fontsize=9, ha="center",
             fontweight="bold", color=C_BLUE_EC)
    d1 = box(ax2, 4.5, 2.0, 1.0, 0.35, "disc=5%", C_ORANGE, C_ORANGE_EC, fontsize=8)
    d2 = box(ax2, 5.7, 2.0, 1.0, 0.35, "disc=8%", C_ORANGE, C_ORANGE_EC, fontsize=8)
    arrow(ax2, s2["cx"]-0.1, s2["b"]-0.15, d1["cx"], d1["t"], color=C_BLUE_EC)
    arrow(ax2, s2["cx"]+0.4, s2["b"]-0.15, d2["cx"], d2["t"], color=C_BLUE_EC)

    res2 = box(ax2, 4.5, 1.3, 2.2, 0.35, "emit (ok, sk, disc) rows", C_GREY, C_GREY_EC,
               fontsize=8)
    arrow(ax2, d1["cx"]+0.3, d1["b"], res2["cx"]-0.3, res2["t"])
    arrow(ax2, d2["cx"]-0.3, d2["b"], res2["cx"]+0.3, res2["t"])

    ax2.text(3.5, 0.7, "Prefix (ok, sk) searched once",
             ha="center", fontsize=11, color=C_GREEN_EC, fontweight="bold")
    ax2.text(3.5, 0.3, "Each query searches only its own 3rd variable",
             ha="center", fontsize=10, color="#444444")

    save(fig, "sharing_prefix")


if __name__ == "__main__":
    print("Generating sharing diagrams...")
    fig_trie_caching()
    fig_spooling()
    fig_prefix_sharing()
    print("Done.")
