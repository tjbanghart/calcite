#!/usr/bin/env python3
"""Generate simple graph diagrams showing the three query types."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

OUT = "figures/"

# Colors
C_NODE = "#A8C8E8"
C_NODE_EC = "#4477AA"
C_EDGE = "#555555"
C_TRIANGLE = "#EE7733"
C_HUB = "#FFCC88"
C_HUB_EC = "#EE7733"
C_TABLE = "#D4E8D4"
C_TABLE_EC = "#228833"
C_FK = "#FFAAAA"
C_FK_EC = "#CC4444"


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT + name + ".png", dpi=600, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    print(f"  {name}.png")


def draw_node(ax, x, y, label, color=C_NODE, ec=C_NODE_EC, size=0.3, fontsize=11):
    circle = plt.Circle((x, y), size, facecolor=color, edgecolor=ec, linewidth=1.5, zorder=3)
    ax.add_patch(circle)
    ax.text(x, y, label, ha="center", va="center", fontsize=fontsize,
            fontweight="bold", zorder=4)
    return (x, y)


def draw_arrow(ax, x1, y1, x2, y2, color=C_EDGE, lw=1.5, offset=0.3):
    """Draw arrow between two node centers, shortened by offset to not overlap circles."""
    dx, dy = x2 - x1, y2 - y1
    dist = np.sqrt(dx**2 + dy**2)
    if dist == 0:
        return
    ux, uy = dx/dist, dy/dist
    ax.annotate("", xy=(x2 - ux*offset, y2 - uy*offset),
                xytext=(x1 + ux*offset, y1 + uy*offset),
                arrowprops=dict(arrowstyle="-|>", color=color, lw=lw))


def draw_double_arrow(ax, x1, y1, x2, y2, color=C_EDGE, lw=1.5, offset=0.3):
    """Draw bidirectional arrow (for undirected-style edges)."""
    draw_arrow(ax, x1, y1, x2, y2, color, lw, offset)
    draw_arrow(ax, x2, y2, x1, y1, color, lw, offset)


def draw_table_box(ax, x, y, w, h, name, columns, color=C_TABLE, ec=C_TABLE_EC):
    """Draw a simplified table representation."""
    # Header
    rect = mpatches.FancyBboxPatch((x, y+h*0.6), w, h*0.4, boxstyle="round,pad=0.05",
                                    facecolor=ec, edgecolor=ec, linewidth=1.5)
    ax.add_patch(rect)
    ax.text(x + w/2, y + h*0.8, name, ha="center", va="center",
            fontsize=11, fontweight="bold", color="white")
    # Body
    rect2 = mpatches.FancyBboxPatch((x, y), w, h*0.6, boxstyle="round,pad=0.05",
                                     facecolor=color, edgecolor=ec, linewidth=1.5)
    ax.add_patch(rect2)
    ax.text(x + w/2, y + h*0.3, columns, ha="center", va="center",
            fontsize=8, color="#333333", fontstyle="italic")
    return dict(cx=x+w/2, t=y+h, b=y, l=x, r=x+w)


# =========================================================================
# 1. Synthetic triangle query
# =========================================================================
def fig_synthetic():
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    for ax in [ax1, ax2]:
        ax.set_xlim(-0.5, 7.5)
        ax.set_ylim(-0.5, 5.5)
        ax.axis("off")

    # --- LEFT: The graph with a triangle highlighted ---
    ax1.set_title("The Graph", fontsize=14, fontweight="bold", pad=10)

    # Nodes
    a = draw_node(ax1, 1, 4.5, "A", C_NODE, C_NODE_EC)
    b = draw_node(ax1, 5, 4.5, "B", C_NODE, C_NODE_EC)
    c = draw_node(ax1, 3, 1.5, "C", C_NODE, C_NODE_EC)
    d = draw_node(ax1, 6.5, 2.5, "D", C_NODE, C_NODE_EC)
    e = draw_node(ax1, 0, 2, "E", C_NODE, C_NODE_EC)

    # Background edges (grey)
    for n1, n2 in [(a, b), (b, d), (a, e), (e, c), (d, c), (c, a)]:
        draw_arrow(ax1, n1[0], n1[1], n2[0], n2[1], color="#CCCCCC", lw=1.0)

    # Highlighted triangle: A -> B -> C -> A
    draw_arrow(ax1, a[0], a[1], b[0], b[1], color=C_TRIANGLE, lw=3, offset=0.32)
    draw_arrow(ax1, b[0], b[1], c[0], c[1], color=C_TRIANGLE, lw=3, offset=0.32)
    draw_arrow(ax1, c[0], c[1], a[0], a[1], color=C_TRIANGLE, lw=3, offset=0.32)

    ax1.text(3, 0.3, "Query: find all triangles", ha="center",
             fontsize=12, color=C_TRIANGLE, fontweight="bold")

    # --- RIGHT: Why binary joins blow up ---
    ax2.set_title("Why Binary Joins Struggle", fontsize=14, fontweight="bold", pad=10)

    # Step 1: R JOIN S
    ax2.text(3.5, 4.8, "Step 1: R(a,b) JOIN S(b,c)", fontsize=12,
             ha="center", fontweight="bold")
    ax2.text(3.5, 4.2, "Produces ALL 2-hop paths: a -> b -> c",
             ha="center", fontsize=11, color="#444444")
    ax2.text(3.5, 3.6, "If b is popular (many edges), this explodes",
             ha="center", fontsize=11, color=C_FK_EC, fontweight="bold")

    # Visual: fan-out
    src = draw_node(ax2, 1, 2.5, "a", C_NODE, C_NODE_EC, size=0.25, fontsize=10)
    mid = draw_node(ax2, 3.5, 2.5, "b", C_HUB, C_HUB_EC, size=0.25, fontsize=10)
    for i, dy in enumerate([-0.8, -0.3, 0.2, 0.7, 1.2]):
        dst = draw_node(ax2, 6, 2.5 + dy, f"c{i+1}", "#DDDDDD", "#AAAAAA",
                        size=0.2, fontsize=8)
        draw_arrow(ax2, mid[0], mid[1], dst[0], dst[1], color="#AAAAAA", lw=1, offset=0.25)
    draw_arrow(ax2, src[0], src[1], mid[0], mid[1], color=C_TRIANGLE, lw=2, offset=0.27)

    ax2.text(3.5, 0.8, "Step 2: filter with T(c,a) -- most paths aren't triangles",
             ha="center", fontsize=11, color="#444444")
    ax2.text(3.5, 0.2, "WCOJ: intersect ALL relations at each step, never builds the blowup",
             ha="center", fontsize=10, color=C_NODE_EC, fontweight="bold")

    save(fig, "query_synthetic")


# =========================================================================
# 2. Self-join triangle (TPC-H lineitem)
# =========================================================================
def fig_selfjoin():
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(-0.5, 9.5)
    ax.set_ylim(-0.5, 6)
    ax.axis("off")
    ax.set_title("TPC-H Self-Join: Same Table, Different Keys", fontsize=14,
                 fontweight="bold", pad=15)

    # Three copies of lineitem as nodes in a triangle
    l1 = draw_node(ax, 4.5, 5, "L1", C_HUB, C_HUB_EC, size=0.5, fontsize=13)
    l2 = draw_node(ax, 1.5, 1.5, "L2", C_HUB, C_HUB_EC, size=0.5, fontsize=13)
    l3 = draw_node(ax, 7.5, 1.5, "L3", C_HUB, C_HUB_EC, size=0.5, fontsize=13)

    # Edge labels (join predicates)
    # L1 -- L2 on orderkey
    draw_arrow(ax, l1[0], l1[1], l2[0], l2[1], color=C_TRIANGLE, lw=2.5, offset=0.55)
    ax.text(2.3, 3.7, "same\norderkey", ha="center", fontsize=10,
            color=C_TRIANGLE, fontweight="bold")

    # L2 -- L3 on suppkey
    draw_arrow(ax, l2[0], l2[1], l3[0], l3[1], color=C_TRIANGLE, lw=2.5, offset=0.55)
    ax.text(4.5, 0.7, "same suppkey", ha="center", fontsize=10,
            color=C_TRIANGLE, fontweight="bold")

    # L3 -- L1 on partkey (closes cycle)
    draw_arrow(ax, l3[0], l3[1], l1[0], l1[1], color=C_TRIANGLE, lw=2.5, offset=0.55)
    ax.text(6.7, 3.7, "same\npartkey", ha="center", fontsize=10,
            color=C_TRIANGLE, fontweight="bold")

    # Labels under each node
    ax.text(4.5, 4.2, "lineitem\n(orderkey, suppkey, partkey, ...)",
            ha="center", fontsize=9, color="#444444", fontstyle="italic")
    ax.text(1.5, 0.7, "lineitem\n(same table)", ha="center", fontsize=9,
            color="#444444", fontstyle="italic")
    ax.text(7.5, 0.7, "lineitem\n(same table)", ha="center", fontsize=9,
            color="#444444", fontstyle="italic")

    # Center annotation
    ax.text(4.5, 3.0, "\"Find line items in the\nsame order, from the same\n"
            "supplier, for the same part\"",
            ha="center", fontsize=11, color="#333333",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFFDD", edgecolor="#CCCC88"))

    ax.text(4.5, -0.2, "All three copies are the same table joined on different columns\n"
            "2.9M matching triples at SF=0.01",
            ha="center", fontsize=10, color="#444444")

    save(fig, "query_selfjoin")


# =========================================================================
# 3. FK cycle (customer-orders-lineitem-supplier)
# =========================================================================
def fig_fk_cycle():
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.set_xlim(-0.5, 9.5)
    ax.set_ylim(-0.5, 6)
    ax.axis("off")
    ax.set_title("TPC-H FK Cycle: Chain Across 4 Tables", fontsize=14,
                 fontweight="bold", pad=15)

    # Four tables in a rectangle
    c = draw_node(ax, 1.5, 4.5, "C", C_NODE, C_NODE_EC, size=0.5, fontsize=13)
    o = draw_node(ax, 7.5, 4.5, "O", C_NODE, C_NODE_EC, size=0.5, fontsize=13)
    l = draw_node(ax, 7.5, 1.5, "L", C_NODE, C_NODE_EC, size=0.5, fontsize=13)
    s = draw_node(ax, 1.5, 1.5, "S", C_NODE, C_NODE_EC, size=0.5, fontsize=13)

    # Table names
    ax.text(1.5, 5.3, "customer", ha="center", fontsize=10, fontstyle="italic", color="#444444")
    ax.text(7.5, 5.3, "orders", ha="center", fontsize=10, fontstyle="italic", color="#444444")
    ax.text(7.5, 0.7, "lineitem", ha="center", fontsize=10, fontstyle="italic", color="#444444")
    ax.text(1.5, 0.7, "supplier", ha="center", fontsize=10, fontstyle="italic", color="#444444")

    # FK edges (chain)
    draw_arrow(ax, c[0], c[1], o[0], o[1], color=C_NODE_EC, lw=2.5, offset=0.55)
    ax.text(4.5, 5.0, "custkey", ha="center", fontsize=10, fontweight="bold", color=C_NODE_EC)

    draw_arrow(ax, o[0], o[1], l[0], l[1], color=C_NODE_EC, lw=2.5, offset=0.55)
    ax.text(8.3, 3.0, "orderkey", ha="center", fontsize=10, fontweight="bold", color=C_NODE_EC)

    draw_arrow(ax, l[0], l[1], s[0], s[1], color=C_NODE_EC, lw=2.5, offset=0.55)
    ax.text(4.5, 1.0, "suppkey", ha="center", fontsize=10, fontweight="bold", color=C_NODE_EC)

    # Closing edge (nationkey -- the problematic one)
    draw_arrow(ax, s[0], s[1], c[0], c[1], color=C_FK_EC, lw=3, offset=0.55)
    ax.text(0.3, 3.0, "nationkey\n(closes cycle)", ha="center", fontsize=10,
            fontweight="bold", color=C_FK_EC)

    # Center annotation
    ax.text(4.5, 3.0, "\"Which customers bought\nfrom suppliers in the\nsame country?\"",
            ha="center", fontsize=11, color="#333333",
            bbox=dict(boxstyle="round,pad=0.3", facecolor="#FFFFDD", edgecolor="#CCCC88"))

    ax.text(4.5, -0.2, "nationkey has only 25 values -- binary joins prune early\n"
            "WCOJ pays intersection cost at every level anyway (1.5x slower)",
            ha="center", fontsize=10, color=C_FK_EC)

    save(fig, "query_fk_cycle")


if __name__ == "__main__":
    print("Generating query diagrams...")
    fig_synthetic()
    fig_selfjoin()
    fig_fk_cycle()
    print("Done.")
