#!/usr/bin/env python3
"""
generate_charts.py
==================

Reads all election_report.csv files recursively from the results directory
and produces five publication-quality PNG charts.

Usage:
    python generate_charts.py <results_dir> <charts_output_dir>

Expected results_dir structure:
    results/
        3_nodes/round_1/election_report.csv  ...
        5_nodes/...
        7_nodes/...
"""

import glob
import os
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns


# ─────────────────────────────────────────────────────────────────────────────
#  Style constants
# ─────────────────────────────────────────────────────────────────────────────

PALETTE      = "Set1"
GRID_ALPHA   = 0.35
TITLE_SIZE   = 15
LABEL_SIZE   = 12
LEGEND_SIZE  = 10
DPI          = 150
FIGSIZE_WIDE = (12, 7)
FIGSIZE_TALL = (14, 8)

COL_MAP = {
    "Leader Failure Detection Duration (s)": "Failure Detection",
    "Leader Election Duration (s)":          "Election Duration",
    "Out of Service Time (s)":               "Out of Service",
}


# ─────────────────────────────────────────────────────────────────────────────
#  Data extraction
# ─────────────────────────────────────────────────────────────────────────────

def _infer_cluster_size(csv_path):
    """Try path name first, then CSV header."""
    for part in Path(csv_path).parts:
        m = re.match(r"^(\d+)_nodes$", part)
        if m:
            return int(m.group(1))
    try:
        with open(csv_path, encoding="utf-8") as f:
            f.readline()
            row1 = f.readline()
        return int(row1.split(",")[1].strip())
    except Exception:
        return None


def extract_data(results_dir):
    """
    Recursively find all election_report.csv files and extract metric rows.

    Returns
    -------
    df_metrics  : DataFrame  [Cluster Size, Failure Detection,
                               Election Duration, Out of Service]
    df_recovery : DataFrame  [Cluster Size, Node ID, Downtime, Recovery Time]
    """
    pattern      = os.path.join(results_dir, "**", "election_report.csv")
    report_files = glob.glob(pattern, recursive=True)
    print("Found {} election_report.csv file(s).".format(len(report_files)))

    cluster_metrics = []
    node_recovery   = []

    for path in report_files:
        try:
            cluster_size = _infer_cluster_size(path)
            if cluster_size is None:
                print("  Warning: cannot determine cluster size for {} — skipping.".format(path))
                continue

            df = pd.read_csv(path, skiprows=3)
            df.columns = [c.strip() for c in df.columns]

            for _, row in df.iterrows():
                if pd.isna(row.get("Term Number")):
                    continue

                metrics = {"Cluster Size": cluster_size}
                for csv_col, name in COL_MAP.items():
                    raw = row.get(csv_col, np.nan)
                    if isinstance(raw, str) and raw.strip().upper() in ("NA", "NONE", ""):
                        metrics[name] = np.nan
                    else:
                        try:
                            metrics[name] = float(raw)
                        except Exception:
                            metrics[name] = np.nan
                cluster_metrics.append(metrics)

                # Parse recovery info
                rec_col  = "Rejoining Nodes & Recovery Time (s)"
                rec_info = str(row.get(rec_col, ""))
                if rec_info.lower() not in ("none", "nan", ""):
                    for m in re.finditer(
                        r"Node\s+(\S+):\s+downtime\s+([\d.]+)s,\s+recovery\s+([\d.]+)s",
                        rec_info,
                    ):
                        node_recovery.append({
                            "Cluster Size":  cluster_size,
                            "Node ID":       m.group(1),
                            "Downtime":      float(m.group(2)),
                            "Recovery Time": float(m.group(3)),
                        })

        except Exception as exc:
            print("  Warning: error reading {}: {}".format(path, exc))

    return pd.DataFrame(cluster_metrics), pd.DataFrame(node_recovery)


# ─────────────────────────────────────────────────────────────────────────────
#  Helper: check if a DataFrame has usable numeric data
# ─────────────────────────────────────────────────────────────────────────────

def _has_data(df, cols=None):
    if df is None or df.empty:
        return False
    if cols:
        for c in cols:
            if c in df.columns and df[c].notna().any():
                return True
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 1 — Average metrics vs cluster size
# ─────────────────────────────────────────────────────────────────────────────

def plot_metrics(df, out_dir):
    metric_cols = list(COL_MAP.values())
    if not _has_data(df, metric_cols):
        print("  No metrics data — skipping cluster_performance.png")
        return

    avg = (
        df.groupby("Cluster Size")[metric_cols]
        .mean()
        .reset_index()
        .sort_values("Cluster Size")
    )

    # Drop columns that are entirely NaN
    valid_metrics = [c for c in metric_cols if avg[c].notna().any()]
    if not valid_metrics:
        print("  All metric columns are NaN — skipping cluster_performance.png")
        return

    print("\nAverage metrics per cluster size:")
    print(avg[["Cluster Size"] + valid_metrics].to_string(index=False))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    styles  = [("-", "o"), ("--", "s"), ("-.", "^")]
    colors  = sns.color_palette(PALETTE, len(valid_metrics))

    for (metric, (ls, mk), color) in zip(valid_metrics, styles, colors):
        ax.plot(
            avg["Cluster Size"], avg[metric],
            marker=mk, linestyle=ls, color=color,
            linewidth=2.5, markersize=9,
            label="Avg {}".format(metric),
        )
        for x, y in zip(avg["Cluster Size"], avg[metric]):
            if not np.isnan(y):
                ax.annotate(
                    "{:.3f}s".format(y),
                    xy=(x, y), xytext=(0, 9),
                    textcoords="offset points",
                    ha="center", fontsize=8.5, color=color,
                )

    ax.set_title("etcd Raft Performance Metrics vs Cluster Size",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Cluster Size (Number of Nodes)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Time (seconds)", fontsize=LABEL_SIZE)
    ax.set_xticks(avg["Cluster Size"])
    ax.xaxis.set_major_formatter(mticker.FormatStrFormatter("%d nodes"))
    ax.legend(fontsize=LEGEND_SIZE, framealpha=0.9)
    ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "cluster_performance.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 2 — CDF
# ─────────────────────────────────────────────────────────────────────────────

def plot_cdf(df, out_dir):
    metric_cols = list(COL_MAP.values())
    if not _has_data(df, metric_cols):
        print("  No data for CDF — skipping performance_cdf.png")
        return

    cluster_sizes = sorted(df["Cluster Size"].unique())
    valid_metrics = [c for c in metric_cols if df[c].notna().any()]
    if not valid_metrics or not cluster_sizes:
        print("  No valid CDF data — skipping performance_cdf.png")
        return

    line_styles = ["-", "--", ":"]
    markers     = ["o", "s", "^"]
    palette     = sns.color_palette("bright", len(cluster_sizes))
    color_map   = dict(zip(cluster_sizes, palette))

    sns.set_theme(style="whitegrid")
    plt.figure(figsize=FIGSIZE_TALL)

    plotted = 0
    for size in cluster_sizes:
        df_s = df[df["Cluster Size"] == size]
        for metric, ls, mk in zip(valid_metrics, line_styles, markers):
            data = df_s[metric].dropna().values
            if len(data) == 0:
                continue
            sorted_data = np.sort(data)
            cdf_y       = np.arange(1, len(sorted_data) + 1) / len(sorted_data)
            plt.step(
                sorted_data, cdf_y,
                where="post",
                label="{} nodes — {}".format(size, metric),
                color=color_map[size],
                linestyle=ls, marker=mk,
                markersize=5,
                markevery=max(1, len(data) // 8),
                linewidth=2,
            )
            plotted += 1

    if plotted == 0:
        plt.close()
        print("  No CDF series to plot — skipping performance_cdf.png")
        return

    plt.title("CDF of Raft Performance Metrics by Cluster Size",
              fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    plt.xlabel("Time (seconds)", fontsize=LABEL_SIZE)
    plt.ylabel("Cumulative Probability", fontsize=LABEL_SIZE)
    plt.ylim(0, 1.05)
    plt.legend(bbox_to_anchor=(1.02, 1), loc="upper left",
               fontsize=LEGEND_SIZE - 1, framealpha=0.9)
    plt.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "performance_cdf.png")
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 3 — Recovery time vs downtime
# ─────────────────────────────────────────────────────────────────────────────

def plot_recovery(df, out_dir):
    if not _has_data(df, ["Downtime", "Recovery Time"]):
        print("  No recovery data — skipping node_recovery_performance.png")
        return

    df = df.dropna(subset=["Downtime", "Recovery Time"])
    if df.empty:
        print("  Recovery data is all NaN — skipping node_recovery_performance.png")
        return

    cluster_sizes = sorted(df["Cluster Size"].unique())
    palette       = sns.color_palette("viridis", len(cluster_sizes))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    for size, color in zip(cluster_sizes, palette):
        sub = df[df["Cluster Size"] == size].sort_values("Downtime")
        ax.scatter(sub["Downtime"], sub["Recovery Time"],
                   color=color, s=80, zorder=3,
                   label="{} nodes".format(size))
        if len(sub) > 1:
            ax.plot(sub["Downtime"], sub["Recovery Time"],
                    color=color, linewidth=1.5, alpha=0.6)

    ax.set_title("Node Recovery Time vs Downtime by Cluster Size",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Downtime (seconds)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Recovery Time (seconds)", fontsize=LABEL_SIZE)
    ax.legend(title="Cluster Size", fontsize=LEGEND_SIZE, framealpha=0.9)
    ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "node_recovery_performance.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 4 — Box-plot distribution per metric
# ─────────────────────────────────────────────────────────────────────────────

def plot_boxplot(df, out_dir):
    metric_cols = list(COL_MAP.values())
    if not _has_data(df, metric_cols):
        print("  No data for box-plot — skipping metrics_boxplot.png")
        return

    valid_metrics = [c for c in metric_cols if df[c].notna().any()]
    if not valid_metrics:
        print("  All metric columns NaN — skipping metrics_boxplot.png")
        return

    melted = df.melt(
        id_vars="Cluster Size",
        value_vars=valid_metrics,
        var_name="Metric",
        value_name="Time (s)",
    ).dropna(subset=["Time (s)"])

    if melted.empty:
        print("  Melted data empty — skipping metrics_boxplot.png")
        return

    cluster_sizes = sorted(df["Cluster Size"].unique())
    palette       = sns.color_palette("Set2", len(cluster_sizes))

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, len(valid_metrics),
                              figsize=(5 * len(valid_metrics), 6),
                              sharey=False)
    if len(valid_metrics) == 1:
        axes = [axes]

    for ax, metric in zip(axes, valid_metrics):
        sub = melted[melted["Metric"] == metric]
        if sub.empty:
            ax.set_visible(False)
            continue
        sns.boxplot(
            data=sub, x="Cluster Size", y="Time (s)",
            palette=palette, ax=ax,
            width=0.55, linewidth=1.4,
            flierprops=dict(marker="o", markersize=4, alpha=0.5),
        )
        ax.set_title(metric, fontsize=LABEL_SIZE, fontweight="bold")
        ax.set_xlabel("Cluster Size (nodes)", fontsize=LABEL_SIZE - 1)
        ax.set_ylabel("Time (s)", fontsize=LABEL_SIZE - 1)
        ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    fig.suptitle("Distribution of Raft Timing Metrics Across Rounds",
                 fontsize=TITLE_SIZE, fontweight="bold", y=1.02)
    plt.tight_layout()
    out_path = os.path.join(out_dir, "metrics_boxplot.png")
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 5 — Grouped bar chart
# ─────────────────────────────────────────────────────────────────────────────

def plot_bar_comparison(df, out_dir):
    metric_cols = list(COL_MAP.values())
    if not _has_data(df, metric_cols):
        print("  No data for bar chart — skipping cluster_bar_comparison.png")
        return

    valid_metrics = [c for c in metric_cols if df[c].notna().any()]
    if not valid_metrics:
        return

    avg = (
        df.groupby("Cluster Size")[valid_metrics]
        .mean()
        .reset_index()
        .sort_values("Cluster Size")
    )
    if avg.empty:
        return

    x        = np.arange(len(avg))
    n        = len(valid_metrics)
    width    = 0.22
    offsets  = np.linspace(-(n - 1) / 2 * width, (n - 1) / 2 * width, n)
    colors   = sns.color_palette(PALETTE, n)

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    for i, (metric, color) in enumerate(zip(valid_metrics, colors)):
        vals = avg[metric].values
        bars = ax.bar(
            x + offsets[i], vals,
            width=width, color=color, alpha=0.85,
            label=metric, edgecolor="white", linewidth=0.6,
        )
        for bar, val in zip(bars, vals):
            if not np.isnan(val):
                ax.text(
                    bar.get_x() + bar.get_width() / 2,
                    val + 0.001,
                    "{:.3f}".format(val),
                    ha="center", va="bottom", fontsize=7.5,
                )

    ax.set_xticks(x)
    ax.set_xticklabels(
        ["{} nodes".format(int(s)) for s in avg["Cluster Size"]],
        fontsize=LABEL_SIZE,
    )
    ax.set_xlabel("Cluster Size", fontsize=LABEL_SIZE)
    ax.set_ylabel("Average Time (seconds)", fontsize=LABEL_SIZE)
    ax.set_title("Average Raft Timing by Cluster Size",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.legend(fontsize=LEGEND_SIZE, framealpha=0.9)
    ax.grid(True, axis="y", linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "cluster_bar_comparison.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print("Usage: generate_charts.py <results_dir> <charts_output_dir>")
        sys.exit(1)

    results_dir = sys.argv[1]
    charts_dir  = sys.argv[2]
    os.makedirs(charts_dir, exist_ok=True)

    print("Reading reports from : {}".format(results_dir))
    print("Saving charts to     : {}\n".format(charts_dir))

    try:
        df_metrics, df_recovery = extract_data(results_dir)
    except Exception as exc:
        print("ERROR extracting data: {}".format(exc))
        sys.exit(1)

    print("\nMetrics rows : {}".format(len(df_metrics)))
    print("Recovery rows: {}\n".format(len(df_recovery)))

    plot_metrics(df_metrics,    charts_dir)
    plot_cdf(df_metrics,        charts_dir)
    plot_recovery(df_recovery,  charts_dir)
    plot_boxplot(df_metrics,    charts_dir)
    plot_bar_comparison(df_metrics, charts_dir)

    print("\nChart generation complete.")


if __name__ == "__main__":
    main()
