#!/usr/bin/env python3
"""
generate_charts.py
==================

Reads all election_report.csv files recursively from the results directory
and produces 11 publication-quality PNG charts.

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
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.lines import Line2D


# ─────────────────────────────────────────────────────────────────────────────
#  Style constants
# ─────────────────────────────────────────────────────────────────────────────

PALETTE      = "Set1"
GRID_ALPHA   = 0.35
TITLE_SIZE   = 15
LABEL_SIZE   = 12
LEGEND_SIZE  = 10
DPI          = 180
FIGSIZE_WIDE = (13, 7)
FIGSIZE_TALL = (14, 8)
FIGSIZE_SQ   = (12, 10)

COL_MAP = {
    "Leader Failure Detection Duration (s)": "Failure Detection",
    "Leader Election Duration (s)":          "Election Duration",
    "Out of Service Time (s)":               "Out of Service",
}

FMT6 = "{:.6f}"   # 6-decimal label format


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


def _infer_round(csv_path):
    """Extract round number from path, e.g. round_3 → 3."""
    for part in Path(csv_path).parts:
        m = re.match(r"^round_(\d+)$", part)
        if m:
            return int(m.group(1))
    return 0


def extract_data(results_dir):
    """
    Recursively find all election_report.csv files and extract metric rows.

    Returns
    -------
    df_metrics   : DataFrame [Cluster Size, Round, Failure Index, Term,
                               Failure Detection, Election Duration,
                               Out of Service, Avg HB Interval, HB Std Dev]
    df_recovery  : DataFrame [Cluster Size, Round, Node ID, Downtime, Recovery Time]
    df_leader    : DataFrame [Cluster Size, Round, Failure Index,
                               Failed Node, New Leader]
    """
    pattern      = os.path.join(results_dir, "**", "election_report.csv")
    report_files = glob.glob(pattern, recursive=True)
    print("Found {} election_report.csv file(s).".format(len(report_files)))

    cluster_metrics = []
    node_recovery   = []
    leader_info     = []

    for path in report_files:
        try:
            cluster_size = _infer_cluster_size(path)
            round_num    = _infer_round(path)
            if cluster_size is None:
                print("  Warning: cannot determine cluster size for {} — skipping.".format(path))
                continue

            df = pd.read_csv(path, skiprows=3)
            df.columns = [c.strip() for c in df.columns]

            for _, row in df.iterrows():
                if pd.isna(row.get("Term Number")):
                    continue

                fi = int(row.get("Failure Index", 0) or 0)

                metrics = {
                    "Cluster Size":  cluster_size,
                    "Round":         round_num,
                    "Failure Index": fi,
                    "Term":          int(row.get("Term Number", 0) or 0),
                }
                for csv_col, name in COL_MAP.items():
                    raw = row.get(csv_col, np.nan)
                    if isinstance(raw, str) and raw.strip().upper() in ("NA", "NONE", ""):
                        metrics[name] = np.nan
                    else:
                        try:
                            metrics[name] = float(raw)
                        except Exception:
                            metrics[name] = np.nan

                # Heartbeat stats
                for col, key in [
                    ("Average Heartbeat Interval (s)", "Avg HB Interval"),
                    ("Heartbeat Interval Std Dev (s)",  "HB Std Dev"),
                ]:
                    raw = row.get(col, np.nan)
                    if isinstance(raw, str) and raw.strip().upper() in ("NA", "NONE", ""):
                        metrics[key] = np.nan
                    else:
                        try:
                            metrics[key] = float(raw)
                        except Exception:
                            metrics[key] = np.nan

                cluster_metrics.append(metrics)

                # Leader info
                failed_node = row.get("Who Initiated Election", "Unknown")
                new_leader  = row.get("Who Became Leader",      "Unknown")
                leader_info.append({
                    "Cluster Size":  cluster_size,
                    "Round":         round_num,
                    "Failure Index": fi,
                    "Failed Node":   str(failed_node).strip(),
                    "New Leader":    str(new_leader).strip(),
                })

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
                            "Round":         round_num,
                            "Node ID":       m.group(1),
                            "Downtime":      float(m.group(2)),
                            "Recovery Time": float(m.group(3)),
                        })

        except Exception as exc:
            print("  Warning: error reading {}: {}".format(path, exc))

    return (
        pd.DataFrame(cluster_metrics),
        pd.DataFrame(node_recovery),
        pd.DataFrame(leader_info),
    )


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


def _cmap(n):
    """Return n distinct colours from the tab10 palette."""
    return sns.color_palette("tab10", n)


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
    valid_metrics = [c for c in metric_cols if avg[c].notna().any()]
    if not valid_metrics:
        print("  All metric columns are NaN — skipping cluster_performance.png")
        return

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
                    FMT6.format(y) + "s",
                    xy=(x, y), xytext=(0, 9),
                    textcoords="offset points",
                    ha="center", fontsize=7.5, color=color,
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
                    FMT6.format(val),
                    ha="center", va="bottom", fontsize=6.5,
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
#  Chart 6 — Failure Detection Trend across failure cycles
# ─────────────────────────────────────────────────────────────────────────────

def plot_failure_detection_trend(df, out_dir):
    if not _has_data(df, ["Failure Detection", "Failure Index"]):
        print("  No data for failure detection trend — skipping failure_detection_trend.png")
        return

    df_valid = df.dropna(subset=["Failure Detection"])
    if df_valid.empty or "Failure Index" not in df_valid.columns:
        print("  No valid Failure Index data — skipping failure_detection_trend.png")
        return

    cluster_sizes = sorted(df_valid["Cluster Size"].unique())
    colors        = _cmap(len(cluster_sizes))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    for size, color in zip(cluster_sizes, colors):
        sub  = df_valid[df_valid["Cluster Size"] == size]
        grp  = sub.groupby("Failure Index")["Failure Detection"]
        mean = grp.mean()
        std  = grp.std().fillna(0)
        idxs = mean.index.to_numpy()

        ax.plot(idxs, mean.values, marker="o", linewidth=2.5,
                markersize=8, color=color, label="{} nodes".format(size))
        ax.fill_between(idxs,
                         mean.values - std.values,
                         mean.values + std.values,
                         color=color, alpha=0.15)

    ax.set_title("Failure Detection Time across Kill Cycles\n(± 1σ band)",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Failure Index within Round (1 = first kill)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Failure Detection Duration (s)", fontsize=LABEL_SIZE)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.legend(title="Cluster Size", fontsize=LEGEND_SIZE, framealpha=0.9)
    ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "failure_detection_trend.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 7 — Failure-Recovery Waterfall (stacked horizontal bars)
# ─────────────────────────────────────────────────────────────────────────────

def plot_failure_timeline_waterfall(df, out_dir):
    required = ["Failure Detection", "Election Duration", "Out of Service",
                "Cluster Size", "Round", "Failure Index"]
    if not _has_data(df, ["Failure Detection", "Election Duration"]):
        print("  No data for waterfall — skipping failure_timeline_waterfall.png")
        return

    df_v = df.dropna(subset=["Failure Detection", "Election Duration"]).copy()
    if df_v.empty:
        print("  Waterfall: no complete rows — skipping.")
        return

    # Recovery gap = Out of Service − Failure Detection − Election Duration
    df_v["Recovery Gap"] = (
        df_v["Out of Service"]
        .sub(df_v["Failure Detection"])
        .sub(df_v["Election Duration"])
        .clip(lower=0)
    )

    labels = []
    left   = []
    det    = []
    elec   = []
    rec    = []

    for _, row in df_v.iterrows():
        labels.append(
            "{}-node · R{} · F{}".format(
                int(row["Cluster Size"]),
                int(row.get("Round", 0)),
                int(row.get("Failure Index", 0)),
            )
        )
        left.append(0)
        det.append(row["Failure Detection"])
        elec.append(row["Election Duration"])
        rec.append(row.get("Recovery Gap", 0))

    n = len(labels)
    if n == 0:
        print("  Waterfall: no rows to plot — skipping.")
        return

    y_pos = np.arange(n)
    h     = max(0.4, min(0.8, 12.0 / n))   # adaptive bar height

    sns.set_theme(style="whitegrid")
    fig_h = max(6, n * (h + 0.15) + 2)
    fig, ax = plt.subplots(figsize=(13, fig_h))

    det_arr  = np.array(det,  dtype=float)
    elec_arr = np.array(elec, dtype=float)
    rec_arr  = np.array(rec,  dtype=float)

    ax.barh(y_pos, det_arr,  height=h, color="#e74c3c", label="Detection gap")
    ax.barh(y_pos, elec_arr, height=h, left=det_arr,
            color="#f39c12", label="Election gap")
    ax.barh(y_pos, rec_arr,  height=h, left=det_arr + elec_arr,
            color="#27ae60", label="Recovery gap")

    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, fontsize=max(6, 10 - n // 10))
    ax.set_xlabel("Cumulative Time (s)", fontsize=LABEL_SIZE)
    ax.set_title("Failure-Recovery Timeline Waterfall\n(Detection | Election | Recovery)",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.legend(fontsize=LEGEND_SIZE, framealpha=0.9)
    ax.grid(True, axis="x", linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "failure_timeline_waterfall.png")
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 8 — Heartbeat Interval Violin + swarm
# ─────────────────────────────────────────────────────────────────────────────

def plot_heartbeat_violin(df, out_dir):
    if not _has_data(df, ["Avg HB Interval"]):
        print("  No heartbeat data — skipping heartbeat_interval_violin.png")
        return

    df_v = df.dropna(subset=["Avg HB Interval"])
    if df_v.empty:
        print("  Heartbeat violin: all NaN — skipping.")
        return

    cluster_sizes = sorted(df_v["Cluster Size"].unique())
    palette       = sns.color_palette("muted", len(cluster_sizes))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    sns.violinplot(
        data=df_v, x="Cluster Size", y="Avg HB Interval",
        palette=palette, ax=ax,
        inner="quartile", linewidth=1.5, cut=0,
    )
    sns.stripplot(
        data=df_v, x="Cluster Size", y="Avg HB Interval",
        color="black", alpha=0.4, size=4, jitter=True, ax=ax,
    )

    ax.set_title("Heartbeat Interval Distribution by Cluster Size",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Cluster Size (nodes)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Avg Heartbeat Interval (s)", fontsize=LABEL_SIZE)
    ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "heartbeat_interval_violin.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 9 — Election Correlation scatter (Detection vs Election Duration)
# ─────────────────────────────────────────────────────────────────────────────

def plot_election_correlation(df, out_dir):
    if not _has_data(df, ["Failure Detection", "Election Duration"]):
        print("  No data for correlation scatter — skipping election_correlation.png")
        return

    df_v = df.dropna(subset=["Failure Detection", "Election Duration"])
    if df_v.empty:
        print("  Correlation: all NaN — skipping.")
        return

    cluster_sizes = sorted(df_v["Cluster Size"].unique())
    colors        = _cmap(len(cluster_sizes))

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=FIGSIZE_WIDE)

    for size, color in zip(cluster_sizes, colors):
        sub = df_v[df_v["Cluster Size"] == size]
        x   = sub["Failure Detection"].values
        y   = sub["Election Duration"].values

        ax.scatter(x, y, color=color, s=70, alpha=0.75, zorder=3,
                   label="{} nodes".format(size))

        if len(x) > 1:
            coeffs  = np.polyfit(x, y, 1)
            x_line  = np.linspace(x.min(), x.max(), 100)
            y_line  = np.polyval(coeffs, x_line)
            # R²
            y_hat   = np.polyval(coeffs, x)
            ss_res  = np.sum((y - y_hat) ** 2)
            ss_tot  = np.sum((y - y.mean()) ** 2)
            r2      = 1 - ss_res / ss_tot if ss_tot > 0 else float("nan")
            ax.plot(x_line, y_line, color=color, linewidth=1.8, linestyle="--",
                    label="  R²={:.4f}".format(r2))

    ax.set_title("Failure Detection vs Election Duration\n(per cluster size with regression)",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Failure Detection Duration (s)", fontsize=LABEL_SIZE)
    ax.set_ylabel("Election Duration (s)", fontsize=LABEL_SIZE)
    ax.legend(fontsize=LEGEND_SIZE - 1, framealpha=0.9)
    ax.grid(True, linestyle="--", alpha=GRID_ALPHA)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "election_correlation.png")
    plt.savefig(out_path, dpi=DPI)
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 10 — Leader node frequency (killed vs won)
# ─────────────────────────────────────────────────────────────────────────────

def plot_leader_node_frequency(df_leader, out_dir):
    if not _has_data(df_leader):
        print("  No leader data — skipping leader_node_frequency.png")
        return

    df_v = df_leader.dropna(subset=["Failed Node", "New Leader"])
    if df_v.empty:
        print("  Leader frequency: no valid rows — skipping.")
        return

    cluster_sizes = sorted(df_v["Cluster Size"].unique())
    n_sizes       = len(cluster_sizes)

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, n_sizes,
                              figsize=(max(10, 5 * n_sizes), 6),
                              sharey=False)
    if n_sizes == 1:
        axes = [axes]

    for ax, size in zip(axes, cluster_sizes):
        sub     = df_v[df_v["Cluster Size"] == size]
        all_nodes = sorted(set(sub["Failed Node"].tolist() + sub["New Leader"].tolist()))

        killed = sub["Failed Node"].value_counts().reindex(all_nodes, fill_value=0)
        won    = sub["New Leader"].value_counts().reindex(all_nodes, fill_value=0)

        x      = np.arange(len(all_nodes))
        width  = 0.35

        ax.bar(x - width / 2, killed.values, width, label="Times killed (was leader)",
               color="#e74c3c", alpha=0.85, edgecolor="white")
        ax.bar(x + width / 2, won.values,    width, label="Times won election",
               color="#2ecc71", alpha=0.85, edgecolor="white")

        ax.set_title("{} nodes".format(size), fontsize=LABEL_SIZE, fontweight="bold")
        ax.set_xticks(x)
        ax.set_xticklabels(all_nodes, rotation=45, ha="right", fontsize=9)
        ax.set_ylabel("Count", fontsize=LABEL_SIZE - 1)
        ax.legend(fontsize=LEGEND_SIZE - 1)
        ax.grid(True, axis="y", linestyle="--", alpha=GRID_ALPHA)

    fig.suptitle("Leader Node Frequency — Failures vs Election Wins",
                 fontsize=TITLE_SIZE, fontweight="bold", y=1.02)
    plt.tight_layout()
    out_path = os.path.join(out_dir, "leader_node_frequency.png")
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close()
    print("  Saved: {}".format(out_path))


# ─────────────────────────────────────────────────────────────────────────────
#  Chart 11 — Out-of-service Heatmap  (cluster×round  vs  failure index)
# ─────────────────────────────────────────────────────────────────────────────

def plot_failure_heatmap(df, out_dir):
    if not _has_data(df, ["Out of Service"]):
        print("  No OOS data — skipping failure_heatmap.png")
        return

    df_v = df.dropna(subset=["Out of Service"]).copy()
    if df_v.empty or "Failure Index" not in df_v.columns:
        print("  Heatmap: no valid OOS rows — skipping.")
        return

    df_v["Row Label"] = df_v.apply(
        lambda r: "{}-node R{}".format(int(r["Cluster Size"]), int(r.get("Round", 0))),
        axis=1,
    )

    try:
        pivot = df_v.pivot_table(
            index="Row Label",
            columns="Failure Index",
            values="Out of Service",
            aggfunc="mean",
        )
    except Exception as exc:
        print("  Heatmap pivot error: {} — skipping.".format(exc))
        return

    if pivot.empty:
        print("  Heatmap: empty pivot — skipping.")
        return

    sns.set_theme(style="white")
    fig_h = max(5, len(pivot) * 0.55 + 2)
    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 1.8), fig_h))

    sns.heatmap(
        pivot,
        ax=ax,
        cmap="YlOrRd",
        annot=True,
        fmt=".6f",
        linewidths=0.4,
        linecolor="white",
        cbar_kws={"label": "Out of Service Time (s)"},
    )
    ax.set_title("Out-of-Service Time Heatmap\n(rows = cluster × round, columns = failure index)",
                 fontsize=TITLE_SIZE, fontweight="bold", pad=14)
    ax.set_xlabel("Failure Index within Round", fontsize=LABEL_SIZE)
    ax.set_ylabel("Cluster × Round", fontsize=LABEL_SIZE)
    ax.tick_params(axis="y", labelsize=9)

    plt.tight_layout()
    out_path = os.path.join(out_dir, "failure_heatmap.png")
    plt.savefig(out_path, dpi=DPI, bbox_inches="tight")
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
        df_metrics, df_recovery, df_leader = extract_data(results_dir)
    except Exception as exc:
        print("ERROR extracting data: {}".format(exc))
        sys.exit(1)

    print("\nMetrics rows : {}".format(len(df_metrics)))
    print("Recovery rows: {}".format(len(df_recovery)))
    print("Leader rows  : {}\n".format(len(df_leader)))

    # ── Original 5 charts ──────────────────────────────────────────────────
    plot_metrics(df_metrics,     charts_dir)
    plot_cdf(df_metrics,         charts_dir)
    plot_recovery(df_recovery,   charts_dir)
    plot_boxplot(df_metrics,     charts_dir)
    plot_bar_comparison(df_metrics, charts_dir)

    # ── New 6 charts ───────────────────────────────────────────────────────
    plot_failure_detection_trend(df_metrics,        charts_dir)
    plot_failure_timeline_waterfall(df_metrics,     charts_dir)
    plot_heartbeat_violin(df_metrics,               charts_dir)
    plot_election_correlation(df_metrics,           charts_dir)
    plot_leader_node_frequency(df_leader,           charts_dir)
    plot_failure_heatmap(df_metrics,                charts_dir)

    print("\nChart generation complete.")


if __name__ == "__main__":
    main()
