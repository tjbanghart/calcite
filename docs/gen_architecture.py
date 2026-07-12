#!/usr/bin/env python3
"""Generate the system architecture diagram for Section 3.0."""

import matplotlib
matplotlib.use("pgf")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

plt.rcParams.update({
    "pgf.texsystem": "pdflatex",
    "font.family": "serif",
    "font.size": 8,
    "figure.dpi": 300,
})

fig, ax = plt.subplots(figsize=(7.5, 4.5))
ax.set_xlim(0, 10)
ax.set_ylim(0, 6)
ax.axis("off")

# Colors -- all new components use the same blue palette
C_EXISTING = "#D4D4D4"   # existing Calcite (grey)
C_EX_EC    = "#999999"
C_NEW      = "#4477AA"   # new components border (blue)
C_NEW_LT   = "#A8C8E8"   # new component fill (light blue)
C_ARROW    = "#555555"

# ---------- helpers --------------------------------------------------------

def box(x, y, w, h, label, fc, ec, fontsize=8, bold=False, sublabel=None):
    """Draw a rounded box and return its (cx, top, bottom) for arrow anchoring."""
    rect = mpatches.FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0.08",
        facecolor=fc, edgecolor=ec, linewidth=1.2)
    ax.add_patch(rect)
    weight = "bold" if bold else "normal"
    ax.text(x + w/2, y + h/2 + (0.1 if sublabel else 0), label,
            ha="center", va="center", fontsize=fontsize, fontweight=weight)
    if sublabel:
        ax.text(x + w/2, y + h/2 - 0.18, sublabel,
                ha="center", va="center", fontsize=6.5, fontstyle="italic",
                color="#444444")
    # Return anchor points: (left_cx, right_cx, cx, top, bottom)
    return dict(l=x, r=x+w, cx=x+w/2, t=y+h, b=y)

def arrow(x1, y1, x2, y2):
    ax.annotate("", xy=(x2, y2), xytext=(x1, y1),
                arrowprops=dict(arrowstyle="-|>", color=C_ARROW, lw=1.0))

# ---------- Row 1: SQL input -----------------------------------------------
sql = box(3.5, 5.1, 3.0, 0.55, "SQL with MULTI()", C_NEW_LT, C_NEW, bold=True)

# ---------- Row 2: Parse & Convert -----------------------------------------
parser = box(0.3, 4.0, 2.2, 0.55, "SQL Parser", C_EXISTING, C_EX_EC,
             sublabel="(extended)")
combine_node = box(3.5, 4.0, 2.2, 0.55, "Combine Node", C_NEW_LT, C_NEW,
                   bold=True, sublabel="SqlKind.MULTI")
relconv = box(6.8, 4.0, 2.8, 0.55, "Rel Converter", C_EXISTING, C_EX_EC,
              sublabel="SqlToRelConverter")

# ---------- Row 3: Optimizer ------------------------------------------------
volcano = box(0.3, 2.7, 2.2, 0.55, "VolcanoPlanner", C_EXISTING, C_EX_EC,
              sublabel="(standard rules)")
wcoj_rule = box(3.3, 2.7, 3.0, 0.55, "EnumerableWCOJRule", C_NEW_LT, C_NEW,
                bold=True, sublabel="GYO cyclicity test")
sharing = box(7.0, 2.7, 2.6, 0.55, "Sharing Rules", C_NEW_LT, C_NEW,
              bold=True, sublabel="Spool + Prefix")

# ---------- Row 4: Physical operators ---------------------------------------
binary = box(0.3, 1.3, 2.2, 0.55, "Binary Joins", C_EXISTING, C_EX_EC,
             sublabel="Hash / Merge / NL")
wcoj_op = box(3.0, 1.3, 2.0, 0.55, "EnumerableWCOJ", C_NEW_LT, C_NEW,
              bold=True, sublabel="HashTrie + backtrack")
combine_op = box(5.5, 1.3, 2.2, 0.55, "EnumerableCombine", C_NEW_LT, C_NEW,
                 bold=True, sublabel="TrieCache")
spools = box(8.2, 1.3, 1.4, 0.55, "Spools", C_NEW_LT, C_NEW,
             sublabel="read/write")

# ---------- Row 5: Execution ------------------------------------------------
exec_box = box(2.5, 0.1, 5.0, 0.55, "Linq4j Code Generation + Execution",
               C_EXISTING, C_EX_EC)

# ---------- Arrows: precise anchor-to-anchor --------------------------------

# Row 1 -> Row 2:  SQL feeds into Parser and Combine Node
arrow(sql["cx"] - 0.6, sql["b"], parser["cx"], parser["t"])
arrow(sql["cx"],        sql["b"], combine_node["cx"], combine_node["t"])

# Row 2 horizontal: Parser -> Combine Node -> Rel Converter
arrow(parser["r"], parser["b"] + 0.275, combine_node["l"], combine_node["b"] + 0.275)
arrow(combine_node["r"], combine_node["b"] + 0.275, relconv["l"], relconv["b"] + 0.275)

# Row 2 -> Row 3:  each feeds the optimizer row below it
arrow(parser["cx"],       parser["b"],       volcano["cx"],   volcano["t"])
arrow(combine_node["cx"], combine_node["b"], wcoj_rule["cx"], wcoj_rule["t"])
arrow(relconv["cx"],      relconv["b"],      sharing["cx"],   sharing["t"])

# Row 3 -> Row 4:  optimizer rules produce physical operators
arrow(volcano["cx"],   volcano["b"],   binary["cx"],     binary["t"])
arrow(wcoj_rule["cx"] - 0.4, wcoj_rule["b"], wcoj_op["cx"],    wcoj_op["t"])
arrow(wcoj_rule["cx"] + 0.4, wcoj_rule["b"], combine_op["cx"], combine_op["t"])
arrow(sharing["cx"],   sharing["b"],   spools["cx"],     spools["t"])

# Row 4 -> Row 5:  all physical operators feed execution
arrow(binary["cx"],     binary["b"],     exec_box["cx"] - 1.2, exec_box["t"])
arrow(wcoj_op["cx"],    wcoj_op["b"],    exec_box["cx"] - 0.4, exec_box["t"])
arrow(combine_op["cx"], combine_op["b"], exec_box["cx"] + 0.4, exec_box["t"])
arrow(spools["cx"],     spools["b"],     exec_box["cx"] + 1.2, exec_box["t"])

# ---------- Legend ----------------------------------------------------------
box(8.5, 0.65, 0.35, 0.25, "", C_EXISTING, C_EX_EC)
ax.text(8.95, 0.775, "Existing Calcite", fontsize=6.5, va="center")
box(8.5, 0.25, 0.35, 0.25, "", C_NEW_LT, C_NEW)
ax.text(8.95, 0.375, "New (this work)", fontsize=6.5, va="center")

fig.tight_layout()
fig.savefig("figures/architecture.pgf")
fig.savefig("figures/architecture.pdf")
plt.close(fig)
print("  architecture.pgf / .pdf")
