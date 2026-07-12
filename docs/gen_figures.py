#!/usr/bin/env python3
"""Generate LaTeX-ready figures for the WCOJ+MQO paper."""

import matplotlib
matplotlib.use("pgf")
import matplotlib.pyplot as plt
import numpy as np

# -- Global style ----------------------------------------------------------
plt.rcParams.update({
    "pgf.texsystem": "pdflatex",
    "pgf.preamble": r"\usepackage{amssymb}",
    "font.family": "serif",
    "font.size": 9,
    "axes.labelsize": 9,
    "legend.fontsize": 7.5,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "lines.linewidth": 1.2,
    "lines.markersize": 4,
    "figure.figsize": (4.5, 3.2),   # larger single-column figures
    "figure.dpi": 300,
})

COLORS = {
    "Baseline":      "#888888",
    "WCOJ":          "#4477AA",
    "Combine":       "#EE7733",
    "Combine-Share": "#228833",
    "C-Binary":      "#CCBB44",
}
MARKERS = {
    "Baseline": "s", "WCOJ": "^", "Combine": "o", "Combine-Share": "D",
}

OUT = "figures/"

# ==========================================================================
# Figure 1: Scalability with graph size (Table 2)
# ==========================================================================
def fig_scalability():
    nodes  = [50, 100, 200, 400]
    labels = ["50/300", "100/800", "200/2K", "400/5K"]

    # (mean, std)
    baseline = [(86, 8),   (170, 12),  (265, 44),  (772, 88)]
    wcoj     = [(58, 3),   (156, 41),  (163, 11),  (241, 31)]
    combine  = [(37, 3),   (71, 7),    (116, 15),  (202, 24)]
    cshare   = [(37, 2),   (84, 9),    (137, 24),  (192, 17)]

    fig, ax = plt.subplots(figsize=(5.5, 2.6))
    for name, series in [("Baseline", baseline), ("WCOJ", wcoj),
                         ("Combine", combine), ("Combine-Share", cshare)]:
        means = [s[0] for s in series]
        stds  = [s[1] for s in series]
        ax.errorbar(nodes, means, yerr=stds, marker=MARKERS[name],
                    color=COLORS[name], label=name, capsize=3,
                    capthick=0.8, elinewidth=0.8)

    ax.set_xlabel("Graph size (nodes / edges)")
    ax.set_ylabel("Execution time (ms)")
    ax.set_xticks(nodes)
    ax.set_xticklabels(labels)
    ax.legend(loc="upper left", framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    fig.savefig(OUT + "scalability.pgf")
    fig.savefig(OUT + "scalability.pdf")
    plt.close(fig)
    print("  scalability.pgf / .pdf")


# ==========================================================================
# Figure 2a: Total time vs batch size (Table 3)
# ==========================================================================
def fig_batch_total():
    N = [5, 10, 20]

    # (mean, std)
    baseline = [(274, 33),  (559, 176), (597, 109)]
    wcoj     = [(182, 44),  (297, 38),  (426, 64)]
    combine  = [(110, 9),   (185, 34),  (224, 16)]
    cshare   = [(116, 12),  (165, 23),  (247, 55)]

    fig, ax = plt.subplots(figsize=(5.5, 2.6))
    for name, series in [("Baseline", baseline), ("WCOJ", wcoj),
                         ("Combine", combine), ("Combine-Share", cshare)]:
        means = [s[0] for s in series]
        stds  = [s[1] for s in series]
        ax.errorbar(N, means, yerr=stds, marker=MARKERS[name],
                    color=COLORS[name], label=name, capsize=3,
                    capthick=0.8, elinewidth=0.8)

    ax.set_xlabel("Batch size ($N$)")
    ax.set_ylabel("Total execution time (ms)")
    ax.set_xticks(N)
    ax.legend(loc="upper left", framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    fig.tight_layout()
    fig.savefig(OUT + "batch_total.pgf")
    fig.savefig(OUT + "batch_total.pdf")
    plt.close(fig)
    print("  batch_total.pgf / .pdf")


# ==========================================================================
# Figure 2b: Per-query amortized cost vs batch size (Table 3)
# ==========================================================================
def fig_batch_perquery():
    N = [5, 10, 20]
    combine_pq  = [22, 19, 11]
    cshare_pq   = [23, 17, 12]

    fig, ax = plt.subplots(figsize=(5.5, 2.6))
    x = np.arange(len(N))
    w = 0.3
    ax.bar(x - w/2, combine_pq, w, label="Combine", color=COLORS["Combine"])
    ax.bar(x + w/2, cshare_pq,  w, label="Combine-Share", color=COLORS["Combine-Share"])

    ax.set_xlabel("Batch size ($N$)")
    ax.set_ylabel("Per-query cost (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(N)
    ax.legend(framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)

    # Annotate the drop
    for i, (c, cs) in enumerate(zip(combine_pq, cshare_pq)):
        ax.text(i - w/2, c + 0.5, str(c), ha="center", va="bottom", fontsize=7)
        ax.text(i + w/2, cs + 0.5, str(cs), ha="center", va="bottom", fontsize=7)

    fig.tight_layout()
    fig.savefig(OUT + "batch_perquery.pgf")
    fig.savefig(OUT + "batch_perquery.pdf")
    plt.close(fig)
    print("  batch_perquery.pgf / .pdf")


# ==========================================================================
# Figure 3: TPC-H cyclic join performance (Table 5)
#   3 panels with independent y-axes so each query shape is readable
# ==========================================================================
def _tpch_bar_chart(data, mode_names, filename, fig_label):
    """Helper: draw a grouped bar chart for TPC-H results."""
    plot_data = {k: v for k, v in data.items() if any(x is not None for x in v)}
    ncols = len(plot_data)
    fig, axes = plt.subplots(1, ncols, figsize=(3.5 * ncols, 3.2))
    if ncols == 1:
        axes = [axes]

    for ax, (qname, vals) in zip(axes, plot_data.items()):
        x = np.arange(len(mode_names))
        means = []
        errs  = []
        colors = []
        for i, mode in enumerate(mode_names):
            v = vals[i]
            if v is None:
                means.append(0)
                errs.append(0)
            else:
                means.append(v[0])
                errs.append(v[1])
            colors.append(COLORS[mode])

        bars = ax.bar(x, means, width=0.6, yerr=errs, capsize=2,
                      color=colors, error_kw={"linewidth": 0.8})

        # Mark OOM
        for i, v in enumerate(vals):
            if v is None:
                ax.annotate("OOM", (x[i], ax.get_ylim()[1] * 0.05),
                            ha="center", fontsize=7, fontweight="bold",
                            color=COLORS[mode_names[i]])

        ax.set_xticks(x)
        ax.set_xticklabels([m.replace("Combine-Share", "C-Share")
                            .replace("C-Binary", "C-Bin")
                            for m in mode_names],
                           fontsize=7, rotation=30, ha="right")
        ax.set_title(qname, fontsize=9)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)

    axes[0].set_ylabel("Execution time (s)")
    fig.tight_layout()
    fig.savefig(OUT + filename + ".pgf")
    fig.savefig(OUT + filename + ".pdf")
    plt.close(fig)
    print(f"  {filename}.pgf / .pdf")


def fig_tpch_selfjoin():
    mode_names = ["Baseline", "C-Binary", "WCOJ", "Combine", "Combine-Share"]
    data = {
        "SJ $\\triangle$": [(4.02, 0.36),    (3.58, 0.20),    (2.27, 0.08),    (2.12, 0.09),    (2.18, 0.09)],
        "SJ $\\square$":   [(109.44, 3.32),   None,             (46.53, 2.50),   (49.53, 2.59),   (49.10, 2.51)],
    }
    _tpch_bar_chart(data, mode_names, "tpch_selfjoin",
                    "Self-join cyclic performance")


def fig_tpch_fk():
    mode_names = ["Baseline", "C-Binary", "WCOJ", "Combine", "Combine-Share"]
    data = {
        "FK $\\square$\n(c-o-l-s)":        [(0.902, 0.060),  (1.045, 0.106),  (1.381, 0.091),  (1.291, 0.129),  (1.257, 0.098)],
        "FK $\\diamondsuit$\n(c-o-l-s-n)": [(2.10, 0.05),    (2.28, 0.13),    (8.14, 0.47),    (15.80, 7.23),   (13.23, 4.65)],
    }
    _tpch_bar_chart(data, mode_names, "tpch_fk",
                    "FK cyclic performance")


# ==========================================================================
# Figure 4: Optimization contributions waterfall (Table 4)
# ==========================================================================
def fig_contributions():
    modes = ["Baseline", "+ WCOJ", "+ Combine", "+ C-Share"]

    # Two operating points
    v400 = [772, 241, 202, 192]
    v200 = [559, 297, 185, 165]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(7.5, 2.5), sharey=False)

    colors_bar = ["#888888", "#4477AA", "#EE7733", "#228833"]

    for ax, data, title in [(ax1, v400, "$|V|{=}400,\\; N{=}5$"),
                             (ax2, v200, "$|V|{=}200,\\; N{=}10$")]:
        x = np.arange(len(modes))
        bars = ax.bar(x, data, color=colors_bar, width=0.6)
        ax.set_xticks(x)
        ax.set_xticklabels(modes, fontsize=7, rotation=15, ha="right")
        ax.set_ylabel("Execution time (ms)")
        ax.set_title(title, fontsize=9)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)

        # Annotate speedup over baseline
        for i in range(1, len(data)):
            speedup = data[0] / data[i]
            ax.text(i, data[i] + max(data)*0.03, f"{speedup:.1f}x",
                    ha="center", va="bottom", fontsize=7)

    fig.tight_layout()
    fig.savefig(OUT + "contributions.pgf")
    fig.savefig(OUT + "contributions.pdf")
    plt.close(fig)
    print("  contributions.pgf / .pdf")


# ==========================================================================
if __name__ == "__main__":
    print("Generating figures...")
    fig_scalability()
    fig_batch_total()
    fig_batch_perquery()
    fig_tpch_selfjoin()
    fig_tpch_fk()
    fig_contributions()
    print("Done. Output in", OUT)
