#!/usr/bin/env python3
"""
analyze_election.py
===================

Reads event CSV files produced by run_experiment.py for a single round
and writes a structured election_report.csv compatible with the chart scripts.

Usage:
    python analyze_election.py <round_dir> <output_report.csv> <cluster_size>

    <round_dir> is the round folder, e.g.:
        results/3_nodes/round_1/

    It will read:
        • events.csv            (merged, all nodes)
        • events_node1.csv      (per-node, used for per-node charts)
        • events_node2.csv  …

Input CSV row format (no header):
    timestamp, event, node_id, term, failure_index

    timestamp     : "YYYY-MM-DD HH:MM:SS.ffffff"  (microsecond precision)
    event         : free-text event string (see below)
    node_id       : e.g. "node1"
    term          : integer Raft term
    failure_index : integer 1-4, which kill-cycle this event belongs to

Key events recognised:
    leader failure            - reference time for failure detection
    node failure              - alias for leader failure
    became candidate          - election started (failure detected)
    became leader             - new leader elected
    sent heartbeat to nodeX   - heartbeat sent; tracked for interval calc
    node started              - failed node restarted
    append entries            - log-replication activity (recovery marker)
    received heartbeat        - node is receiving HBs again (recovery marker)
    snapshot                  - node is catching up via snapshot

Output CSV:
    Row 1 (header): Cluster Start Time, No of Nodes, Cluster End Time
    Row 2 (data):   <start_ts>, <N>, <end_ts>
    Row 3:          (blank)
    Row 4 (header): Failure Index, Term Number,
                    Leader Failure Detection Duration (s),
                    Who Initiated Election, Who Became Leader,
                    Leader Election Duration (s), Out of Service Time (s),
                    Average Heartbeat Interval (s),
                    Heartbeat Interval Std Dev (s),
                    Rejoining Nodes & Recovery Time (s)
    Row 5+          one row per term that had election activity
"""

import csv
import glob
import os
import sys
from datetime import datetime
from pathlib import Path
import math


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S.%f")


def _fmt(seconds: float) -> str:
    """Format a duration with 6 decimal places."""
    return "{:.6f}".format(seconds)


# ---------------------------------------------------------------------------
#  Core analysis
# ---------------------------------------------------------------------------

def analyze(round_dir: str, output_csv: str, cluster_size: int) -> None:

    round_path = Path(round_dir)

    # -- Load events from all CSV files in the round dir --------------------
    # Primary source: merged events.csv (all nodes, sorted by orchestrator)
    # We also check per-node files so nothing is missed if merged is empty.
    all_event_files = [round_path / "events.csv"] + sorted(
        round_path.glob("events_node*.csv")
    )

    seen_rows: set[tuple] = set()   # deduplicate across merged + per-node files
    events: list[dict]    = []

    for ev_file in all_event_files:
        if not ev_file.exists():
            continue
        with open(ev_file, "r", newline="", encoding="utf-8") as f:
            for row in csv.reader(f):
                if len(row) < 3:
                    continue
                key = tuple(row)
                if key in seen_rows:
                    continue
                seen_rows.add(key)
                try:
                    events.append({
                        "time":          _parse_ts(row[0]),
                        "event":         row[1].strip(),
                        "node_id":       row[2].strip(),
                        "term":          int(row[3].strip()) if len(row) > 3 else 0,
                        "failure_index": int(row[4].strip()) if len(row) > 4 else 0,
                    })
                except Exception:
                    pass   # skip malformed rows

    if not events:
        print(f"[analyze] No valid events found in {round_dir}")
        _write_empty_report(output_csv, cluster_size)
        return

    events.sort(key=lambda e: e["time"])
    cluster_start = events[0]["time"]
    cluster_end   = events[-1]["time"]

    # -- Build per-(failure_index, term) data structures --------------------
    # Key: (failure_index, term)  — supports repeated terms across cycles
    records: dict[tuple[int, int], dict] = {}
    last_failure_time:  dict[int, datetime]  = {}   # failure_index -> datetime
    last_node_failure:  dict[str, datetime]  = {}   # node_id -> datetime

    def _init_record(fi: int, t: int):
        key = (fi, t)
        if key not in records:
            records[key] = {
                "failure_index":    fi,
                "term":             t,
                "detected_time":    None,
                "initiated_by":     None,
                "leader_time":      None,
                "leader_id":        None,
                "failure_time":     last_failure_time.get(fi),
                "heartbeats":       {},   # dest -> [timestamps]
                "recovering_nodes": [],   # [(node_id, downtime, rec_dur)]
            }

    for idx, e in enumerate(events):
        term = e["term"]
        evt  = e["event"]
        nid  = e["node_id"]
        fi   = e["failure_index"]

        # -- Failure markers
        if "leader failure" in evt or "node failure" in evt:
            last_failure_time[fi]    = e["time"]
            last_node_failure[nid]   = e["time"]

        _init_record(fi, term)

        # -- Node recovery tracking
        if "node started" in evt:
            downtime = None
            if nid in last_node_failure:
                downtime = (e["time"] - last_node_failure[nid]).total_seconds()

            T_start       = e["time"]
            T_end_rec     = None
            T_first_hb    = None
            last_app_time = None

            for nxt in events[idx + 1:]:
                if nxt["node_id"] != nid:
                    continue
                nxt_evt = nxt["event"]
                if "received heartbeat" in nxt_evt or "sent heartbeat" in nxt_evt:
                    if T_first_hb is None:
                        T_first_hb = nxt["time"]
                    ref = last_app_time or T_first_hb
                    if ref and (nxt["time"] - ref).total_seconds() > 0.05:
                        T_end_rec = ref
                        break
                if "append entries" in nxt_evt or "snapshot" in nxt_evt:
                    last_app_time = nxt["time"]

            if T_end_rec is None:
                T_end_rec = last_app_time or T_first_hb or T_start

            rec_dur = (T_end_rec - T_start).total_seconds()
            records[(fi, term)]["recovering_nodes"].append((nid, downtime, rec_dur))

        # -- Candidate detected (election started)
        if "became candidate" in evt:
            rec = records[(fi, term)]
            if rec["detected_time"] is None:
                rec["detected_time"] = e["time"]
                rec["initiated_by"]  = nid

        # -- Leader elected
        if "became leader" in evt:
            rec = records[(fi, term)]
            if rec["leader_time"] is None:
                rec["leader_time"] = e["time"]
                rec["leader_id"]   = nid

        # -- Heartbeat tracking
        if "sent heartbeat to" in evt:
            dest = evt.split("to ")[-1].strip()
            records[(fi, term)]["heartbeats"].setdefault(dest, []).append(e["time"])

    # -- Write report --------------------------------------------------------
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)

        # Header block (3 lines)
        w.writerow(["Cluster Start Time", "No of Nodes", "Cluster End Time"])
        w.writerow([
            cluster_start.strftime("%Y-%m-%d %H:%M:%S.%f"),
            cluster_size,
            cluster_end.strftime("%Y-%m-%d %H:%M:%S.%f"),
        ])
        w.writerow([])

        # Column headers
        w.writerow([
            "Failure Index",
            "Term Number",
            "Leader Failure Detection Duration (s)",
            "Who Initiated Election",
            "Who Became Leader",
            "Leader Election Duration (s)",
            "Out of Service Time (s)",
            "Average Heartbeat Interval (s)",
            "Heartbeat Interval Std Dev (s)",
            "Rejoining Nodes & Recovery Time (s)",
        ])

        # One data row per (failure_index, term) that had election activity,
        # sorted by failure_index first, then term
        for (fi, term) in sorted(records.keys()):
            d = records[(fi, term)]
            if d["detected_time"] is None and d["leader_time"] is None:
                continue   # no election activity in this record

            # Failure detection duration (failure → first candidate)
            det_str = "NA"
            if d["failure_time"] and d["detected_time"]:
                if d["detected_time"] > d["failure_time"]:
                    det_str = _fmt(
                        (d["detected_time"] - d["failure_time"]).total_seconds()
                    )

            # Out-of-service duration (failure → new leader confirmed)
            oos_str = "NA"
            if d["failure_time"] and d["leader_time"]:
                if d["leader_time"] > d["failure_time"]:
                    oos_str = _fmt(
                        (d["leader_time"] - d["failure_time"]).total_seconds()
                    )

            # Election duration (first candidate → new leader)
            elec_str = "NA"
            if d["detected_time"] and d["leader_time"]:
                elec_str = _fmt(
                    (d["leader_time"] - d["detected_time"]).total_seconds()
                )

            # Average + std-dev heartbeat interval
            hb_avg_str = "NA"
            hb_std_str = "NA"
            intervals  = []
            for times in d["heartbeats"].values():
                times_sorted = sorted(times)
                for i in range(1, len(times_sorted)):
                    intervals.append(
                        (times_sorted[i] - times_sorted[i - 1]).total_seconds()
                    )
            if intervals:
                avg = sum(intervals) / len(intervals)
                hb_avg_str = _fmt(avg)
                if len(intervals) > 1:
                    variance = sum((x - avg) ** 2 for x in intervals) / len(intervals)
                    hb_std_str = _fmt(math.sqrt(variance))
                else:
                    hb_std_str = _fmt(0.0)

            # Node recovery summary string
            rec_str = "None"
            if d["recovering_nodes"]:
                parts = []
                for node_id, down, rec in d["recovering_nodes"]:
                    down_s = "{}s".format(_fmt(down)) if down is not None else "Unknown"
                    parts.append(
                        "Node {}: downtime {}, recovery {}s".format(
                            node_id, down_s, _fmt(rec)
                        )
                    )
                rec_str = " | ".join(parts)

            w.writerow([
                fi,
                term,
                det_str,
                d["initiated_by"] or "Unknown",
                d["leader_id"]    or "Unknown",
                elec_str,
                oos_str,
                hb_avg_str,
                hb_std_str,
                rec_str,
            ])

    print("[analyze] Report saved -> {}".format(output_csv))


def _write_empty_report(output_csv: str, cluster_size: int) -> None:
    """Write a structurally valid but empty report so chart scripts skip gracefully."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["Cluster Start Time", "No of Nodes", "Cluster End Time"])
        w.writerow([now_str, cluster_size, now_str])
        w.writerow([])
        w.writerow([
            "Failure Index",
            "Term Number",
            "Leader Failure Detection Duration (s)",
            "Who Initiated Election",
            "Who Became Leader",
            "Leader Election Duration (s)",
            "Out of Service Time (s)",
            "Average Heartbeat Interval (s)",
            "Heartbeat Interval Std Dev (s)",
            "Rejoining Nodes & Recovery Time (s)",
        ])
    print("[analyze] Empty report written -> {}".format(output_csv))


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 4:
        print("Usage: analyze_election.py <round_dir> <output_report.csv> <cluster_size>")
        sys.exit(1)

    round_dir    = sys.argv[1]
    output_csv   = sys.argv[2]
    cluster_size = int(sys.argv[3])

    if not os.path.isdir(round_dir):
        print("Error: round_dir not found: {}".format(round_dir))
        _write_empty_report(output_csv, cluster_size)
        sys.exit(0)

    analyze(round_dir, output_csv, cluster_size)


if __name__ == "__main__":
    main()
