#!/usr/bin/env python3
"""Generate high-resolution PNGs for slides (not LaTeX)."""

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# -- Slide-optimized style (larger fonts, thicker lines) -------------------
plt.rcParams.update({
    "font.family": "sans-serif",
    "font.size": 14,
    "axes.labelsize": 14,
    "legend.fontsize": 12,
    "xtick.labelsize": 12,
    "ytick.labelsize": 12,
    "lines.linewidth": 2.0,
    "lines.markersize": 7,
    "figure.dpi": 600,
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


def save(fig, name):
    fig.tight_layout()
    fig.savefig(OUT + name + ".png", dpi=600, bbox_inches="tight",
                facecolor="white", edgecolor="none")
    plt.close(fig)
    print(f"  {name}.png")


def fig_scalability():
    nodes = [50, 100, 200, 400]
    baseline = [(86, 8),   (170, 12),  (265, 44),  (772, 88)]
    wcoj     = [(58, 3),   (156, 41),  (163, 11),  (241, 31)]
    combine  = [(37, 3),   (71, 7),    (116, 15),  (202, 24)]
    cshare   = [(37, 2),   (84, 9),    (137, 24),  (192, 17)]

    fig, ax = plt.subplots(figsize=(10, 5))
    for name, series in [("Baseline", baseline), ("WCOJ", wcoj),
                         ("Combine", combine), ("Combine-Share", cshare)]:
        means = [s[0] for s in series]
        stds  = [s[1] for s in series]
        ax.errorbar(nodes, means, yerr=stds, marker=MARKERS[name],
                    color=COLORS[name], label=name, capsize=4,
                    capthick=1.2, elinewidth=1.2)
    ax.set_xlabel("Graph size (nodes / edges)")
    ax.set_ylabel("Execution time (ms)")
    ax.set_xticks(nodes)
    ax.set_xticklabels(["50/300", "100/800", "200/2K", "400/5K"])
    ax.legend(loc="upper left", framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    save(fig, "scalability")


def fig_batch_total():
    N = [5, 10, 20]
    baseline = [(274, 33),  (559, 176), (597, 109)]
    wcoj     = [(182, 44),  (297, 38),  (426, 64)]
    combine  = [(110, 9),   (185, 34),  (224, 16)]
    cshare   = [(116, 12),  (165, 23),  (247, 55)]

    fig, ax = plt.subplots(figsize=(10, 5))
    for name, series in [("Baseline", baseline), ("WCOJ", wcoj),
                         ("Combine", combine), ("Combine-Share", cshare)]:
        means = [s[0] for s in series]
        stds  = [s[1] for s in series]
        ax.errorbar(N, means, yerr=stds, marker=MARKERS[name],
                    color=COLORS[name], label=name, capsize=4,
                    capthick=1.2, elinewidth=1.2)
    ax.set_xlabel("Batch size (N)")
    ax.set_ylabel("Total execution time (ms)")
    ax.set_xticks(N)
    ax.legend(loc="upper left", framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    save(fig, "batch_total")


def fig_batch_perquery():
    N = [5, 10, 20]
    combine_pq  = [22, 19, 11]
    cshare_pq   = [23, 17, 12]

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(N))
    w = 0.3
    bars1 = ax.bar(x - w/2, combine_pq, w, label="Combine", color=COLORS["Combine"])
    bars2 = ax.bar(x + w/2, cshare_pq,  w, label="Combine-Share", color=COLORS["Combine-Share"])
    ax.set_xlabel("Batch size (N)")
    ax.set_ylabel("Per-query cost (ms)")
    ax.set_xticks(x)
    ax.set_xticklabels(N)
    ax.legend(framealpha=0.9)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y", linewidth=0.3, alpha=0.5)
    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            ax.annotate(f'{int(h)}', xy=(bar.get_x() + bar.get_width()/2, h),
                        xytext=(0, 4), textcoords="offset points",
                        ha="center", va="bottom", fontsize=12)
    save(fig, "batch_perquery")


def _tpch_bar_chart(data, mode_names, filename):
    plot_data = {k: v for k, v in data.items() if any(x is not None for x in v)}
    ncols = len(plot_data)
    fig, axes = plt.subplots(1, ncols, figsize=(6 * ncols, 5))
    if ncols == 1:
        axes = [axes]

    for ax, (qname, vals) in zip(axes, plot_data.items()):
        x = np.arange(len(mode_names))
        means, errs, colors = [], [], []
        for i, mode in enumerate(mode_names):
            v = vals[i]
            if v is None:
                means.append(0); errs.append(0)
            else:
                means.append(v[0]); errs.append(v[1])
            colors.append(COLORS[mode])

        bars = ax.bar(x, means, width=0.6, yerr=errs, capsize=3,
                      color=colors, error_kw={"linewidth": 1.0})
        for i, v in enumerate(vals):
            if v is None:
                ax.annotate("OOM", (x[i], ax.get_ylim()[1] * 0.05),
                            ha="center", fontsize=11, fontweight="bold",
                            color=COLORS[mode_names[i]])
        ax.set_xticks(x)
        ax.set_xticklabels([m.replace("Combine-Share", "C-Share")
                            .replace("C-Binary", "C-Bin")
                            for m in mode_names],
                           fontsize=11, rotation=30, ha="right")
        ax.set_title(qname, fontsize=14)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)

    axes[0].set_ylabel("Execution time (s)")
    save(fig, filename)


def fig_tpch_selfjoin():
    mode_names = ["Baseline", "C-Binary", "WCOJ", "Combine", "Combine-Share"]
    data = {
        "SJ Triangle":  [(4.02, 0.36), (3.58, 0.20), (2.27, 0.08), (2.12, 0.09), (2.18, 0.09)],
        "SJ 4-Cycle":   [(109.44, 3.32), None, (46.53, 2.50), (49.53, 2.59), (49.10, 2.51)],
    }
    _tpch_bar_chart(data, mode_names, "tpch_selfjoin")


def fig_tpch_fk():
    mode_names = ["Baseline", "C-Binary", "WCOJ", "Combine", "Combine-Share"]
    data = {
        "FK Rectangle\n(c-o-l-s)":    [(0.902, 0.060), (1.045, 0.106), (1.381, 0.091), (1.291, 0.129), (1.257, 0.098)],
        "FK Diamond\n(c-o-l-s-n)":    [(2.10, 0.05), (2.28, 0.13), (8.14, 0.47), (15.80, 7.23), (13.23, 4.65)],
    }
    _tpch_bar_chart(data, mode_names, "tpch_fk")


def fig_contributions():
    modes = ["Baseline", "+ WCOJ", "+ Combine", "+ C-Share"]
    v400 = [772, 241, 202, 192]
    v200 = [559, 297, 185, 165]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5), sharey=False)
    colors_bar = ["#888888", "#4477AA", "#EE7733", "#228833"]

    for ax, data, title in [(ax1, v400, "|V|=400, N=5"),
                             (ax2, v200, "|V|=200, N=10")]:
        x = np.arange(len(modes))
        bars = ax.bar(x, data, color=colors_bar, width=0.6)
        ax.set_xticks(x)
        ax.set_xticklabels(modes, fontsize=11, rotation=15, ha="right")
        ax.set_ylabel("Execution time (ms)")
        ax.set_title(title, fontsize=14)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linewidth=0.3, alpha=0.5)
        for bar, val in zip(bars, data):
            speedup = data[0] / val
            ax.annotate(f'{speedup:.1f}x', xy=(bar.get_x() + bar.get_width()/2, val),
                        xytext=(0, 5), textcoords="offset points",
                        ha="center", va="bottom", fontsize=11)
    save(fig, "contributions")


if __name__ == "__main__":
    print("Generating slide figures (high-res PNG)...")
    fig_scalability()
    fig_batch_total()
    fig_batch_perquery()
    fig_tpch_selfjoin()
    fig_tpch_fk()
    fig_contributions()
    print("Done.")
