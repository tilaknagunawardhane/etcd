#!/usr/bin/env python3
"""
analyze_election.py
===================

Reads a single events.csv produced by run_experiment.py and writes a
structured election_report.csv compatible with the chart scripts.

Usage:
    python analyze_election.py <events.csv> <output_report.csv> <cluster_size>

Input CSV row format (no header):
    timestamp, event, node_id, term

    timestamp  : "YYYY-MM-DD HH:MM:SS.ffffff"
    event      : free-text event string (see below)
    node_id    : e.g. "node1"
    term       : integer Raft term

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
    Row 4 (header): Term Number, Leader Failure Detection Duration (s),
                    Who Initiated Election, Who Became Leader,
                    Leader Election Duration (s), Out of Service Time (s),
                    Average Heartbeat Interval (s),
                    Rejoining Nodes & Recovery Time (s)
    Row 5+          one row per term that had election activity
"""

import csv
import os
import sys
from datetime import datetime


# ---------------------------------------------------------------------------
#  Helpers
# ---------------------------------------------------------------------------

def _parse_ts(ts_str: str) -> datetime:
    return datetime.strptime(ts_str.strip(), "%Y-%m-%d %H:%M:%S.%f")


# ---------------------------------------------------------------------------
#  Core analysis
# ---------------------------------------------------------------------------

def analyze(events_csv: str, output_csv: str, cluster_size: int) -> None:

    # -- Load events ---------------------------------------------------------
    events = []
    with open(events_csv, "r", newline="", encoding="utf-8") as f:
        for row in csv.reader(f):
            if len(row) < 3:
                continue
            try:
                events.append({
                    "time":    _parse_ts(row[0]),
                    "event":   row[1].strip(),
                    "node_id": row[2].strip(),
                    "term":    int(row[3].strip()) if len(row) > 3 else 0,
                })
            except Exception:
                pass  # skip malformed rows

    if not events:
        print(f"[analyze] No valid events found in {events_csv}")
        # Write an empty-but-valid report so chart scripts don't crash
        _write_empty_report(output_csv, cluster_size)
        return

    events.sort(key=lambda e: e["time"])
    cluster_start = events[0]["time"]
    cluster_end   = events[-1]["time"]

    # -- Build per-term data structures --------------------------------------
    terms = {}
    last_failure_time = None
    last_node_failure = {}  # node_id -> datetime of last failure

    def _init_term(t):
        if t not in terms:
            terms[t] = {
                "term":             t,
                "detected_time":    None,   # "became candidate" timestamp
                "initiated_by":     None,   # node_id first candidate
                "leader_time":      None,   # "became leader" timestamp
                "leader_id":        None,   # node_id that became leader
                "failure_time":     last_failure_time,
                "heartbeats":       {},     # dest -> [timestamps]
                "recovering_nodes": [],     # [(node_id, downtime, rec_dur)]
            }

    for idx, e in enumerate(events):
        term = e["term"]
        evt  = e["event"]
        nid  = e["node_id"]

        # -- Failure markers
        if "leader failure" in evt or "node failure" in evt:
            last_failure_time      = e["time"]
            last_node_failure[nid] = e["time"]

        _init_term(term)

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
            terms[term]["recovering_nodes"].append((nid, downtime, rec_dur))

        # -- Candidate detected (election started)
        if "became candidate" in evt:
            if terms[term]["detected_time"] is None:
                terms[term]["detected_time"] = e["time"]
                terms[term]["initiated_by"]  = nid

        # -- Leader elected
        if "became leader" in evt:
            if terms[term]["leader_time"] is None:
                terms[term]["leader_time"] = e["time"]
                terms[term]["leader_id"]   = nid

        # -- Heartbeat tracking
        if "sent heartbeat to" in evt:
            dest = evt.split("to ")[-1].strip()
            terms[term]["heartbeats"].setdefault(dest, []).append(e["time"])

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
            "Term Number",
            "Leader Failure Detection Duration (s)",
            "Who Initiated Election",
            "Who Became Leader",
            "Leader Election Duration (s)",
            "Out of Service Time (s)",
            "Average Heartbeat Interval (s)",
            "Rejoining Nodes & Recovery Time (s)",
        ])

        # One data row per term that had election activity
        for term in sorted(terms):
            d = terms[term]
            if d["detected_time"] is None and d["leader_time"] is None:
                continue  # no election activity in this term

            # Failure detection duration (failure -> first candidate)
            det_str = "NA"
            if d["failure_time"] and d["detected_time"]:
                if d["detected_time"] > d["failure_time"]:
                    det_str = "{:.4f}".format(
                        (d["detected_time"] - d["failure_time"]).total_seconds()
                    )

            # Out-of-service duration (failure -> new leader confirmed)
            oos_str = "NA"
            if d["failure_time"] and d["leader_time"]:
                if d["leader_time"] > d["failure_time"]:
                    oos_str = "{:.4f}".format(
                        (d["leader_time"] - d["failure_time"]).total_seconds()
                    )

            # Election duration (first candidate -> new leader)
            elec_str = "NA"
            if d["detected_time"] and d["leader_time"]:
                elec_str = "{:.4f}".format(
                    (d["leader_time"] - d["detected_time"]).total_seconds()
                )

            # Average heartbeat interval
            hb_str    = "NA"
            intervals = []
            for times in d["heartbeats"].values():
                times.sort()
                for i in range(1, len(times)):
                    intervals.append((times[i] - times[i - 1]).total_seconds())
            if intervals:
                hb_str = "{:.4f}".format(sum(intervals) / len(intervals))

            # Node recovery summary string
            rec_str = "None"
            if d["recovering_nodes"]:
                parts = []
                for node_id, down, rec in d["recovering_nodes"]:
                    down_s = "{:.4f}s".format(down) if down is not None else "Unknown"
                    parts.append(
                        "Node {}: downtime {}, recovery {:.4f}s".format(
                            node_id, down_s, rec
                        )
                    )
                rec_str = " | ".join(parts)

            w.writerow([
                term,
                det_str,
                d["initiated_by"] or "Unknown",
                d["leader_id"]    or "Unknown",
                elec_str,
                oos_str,
                hb_str,
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
            "Term Number",
            "Leader Failure Detection Duration (s)",
            "Who Initiated Election",
            "Who Became Leader",
            "Leader Election Duration (s)",
            "Out of Service Time (s)",
            "Average Heartbeat Interval (s)",
            "Rejoining Nodes & Recovery Time (s)",
        ])
    print("[analyze] Empty report written -> {}".format(output_csv))


# ---------------------------------------------------------------------------
#  Entry point
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 4:
        print("Usage: analyze_election.py <events.csv> <output_report.csv> <cluster_size>")
        sys.exit(1)

    events_csv   = sys.argv[1]
    output_csv   = sys.argv[2]
    cluster_size = int(sys.argv[3])

    if not os.path.exists(events_csv):
        print("Error: events file not found: {}".format(events_csv))
        _write_empty_report(output_csv, cluster_size)
        sys.exit(0)  # Don't fail hard — let the orchestrator continue

    analyze(events_csv, output_csv, cluster_size)


if __name__ == "__main__":
    main()
