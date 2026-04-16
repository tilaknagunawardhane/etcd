#!/usr/bin/env python3
"""
etcd Raft Cluster Performance Benchmark — Main Orchestrator
============================================================

Spins up 3-node, 5-node, and 7-node real etcd clusters using Docker Compose.
For each cluster size, runs N rounds of:
  1. Find the current Raft leader
  2. Perform FAILURES_PER_ROUND kill→re-elect→restart→rejoin cycles
  3. Each cycle: kill the leader, wait for a new leader, restart the failed node
  4. Write per-node events_nodeN.csv + merged events.csv per round
  5. Run analyze_election.py to produce an election_report.csv

After all experiments, calls generate_charts.py to produce PNG graphs.

Usage:
    python run_experiment.py [options]

Options:
    --cluster-sizes  3 5 7   Cluster sizes to test (default: 3 5 7)
    --rounds         4       Rounds per cluster (default: 4)
    --analyze-only           Skip experiments; only (re)run analysis + charts
    --charts-only            Skip experiments + analysis; only regenerate charts
"""

import argparse
import csv
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
#  Paths
# ══════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR  = Path(__file__).parent.resolve()
RESULTS_DIR = SCRIPT_DIR / "results"
CHARTS_DIR  = RESULTS_DIR / "charts"

# ══════════════════════════════════════════════════════════════════════════════
#  Timing / Tuning Constants
# ══════════════════════════════════════════════════════════════════════════════

# Host-side client port for each node index (same across all cluster sizes,
# clusters run sequentially so there are no port conflicts)
CLIENT_PORTS: dict[int, int] = {
    1: 12379, 2: 22379, 3: 32379, 4: 42379,
    5: 52379, 6: 62379, 7: 63379,
}

CLUSTER_STARTUP_TIMEOUT    = 120  # s — max wait for first healthy cluster
INTER_ROUND_STARTUP_TIMEOUT = 60  # s — max wait between rounds
ELECTION_TIMEOUT            = 60  # s — max wait for new leader after kill
RECOVERY_TIMEOUT            = 60  # s — max wait for restarted node to rejoin
POLL_INTERVAL               = 0.2 # s — metrics poll cadence
PRE_KILL_STABILIZE          = 2   # s — baseline data before killing
POST_ELECT_SETTLE           = 2   # s — settle after new leader before restart
POST_RECOVER_SETTLE         = 3   # s — settle after recovery before next failure
INTER_ROUND_PAUSE           = 10  # s — pause between rounds
FAILURES_PER_ROUND          = 4   # number of kill→elect→recover cycles per round


# ══════════════════════════════════════════════════════════════════════════════
#  Logging
# ══════════════════════════════════════════════════════════════════════════════

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

def log(msg: str, indent: int = 0) -> None:
    print(f"[{_ts()}] {'  ' * indent}{msg}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
#  Docker helpers
# ══════════════════════════════════════════════════════════════════════════════

def _run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)

def docker(*args) -> subprocess.CompletedProcess:
    return _run(["docker"] + list(args))

def docker_compose(compose_file: Path, *args, project: str) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", "-f", str(compose_file), "-p", project] + list(args)
    return _run(cmd)

def container_name(cluster_size: int, node_idx: int) -> str:
    """Docker container name matching the `container_name:` in our compose files."""
    return f"etcd{cluster_size}-node{node_idx}"

def stop_container(name: str) -> bool:
    r = docker("stop", name)
    if r.returncode != 0:
        log(f"⚠ docker stop {name}: {r.stderr.strip()}", indent=3)
    return r.returncode == 0

def start_container(name: str) -> bool:
    r = docker("start", name)
    if r.returncode != 0:
        log(f"⚠ docker start {name}: {r.stderr.strip()}", indent=3)
    return r.returncode == 0


# ══════════════════════════════════════════════════════════════════════════════
#  etcd Prometheus metrics helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_raw_metrics(port: int) -> str:
    """GET http://localhost:{port}/metrics, return raw text or empty string."""
    try:
        with urllib.request.urlopen(
            f"http://localhost:{port}/metrics", timeout=2
        ) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception:
        return ""

def _fetch_raft_term(port: int) -> int:
    """
    Fetch the current Raft term from /debug/vars (expvar JSON).
    etcd exposes raft.status there as: {"term": N, "lead": N, ...}
    Falls back to 1 if the endpoint is unavailable (term is always >= 1 in a
    live cluster, so 1 is a safe sentinel that passes the > 0 guard).
    """
    try:
        import json
        with urllib.request.urlopen(
            f"http://localhost:{port}/debug/vars", timeout=2
        ) as resp:
            data = json.loads(resp.read())
        raft_status = data.get("raft.status") or {}
        if isinstance(raft_status, str):
            raft_status = json.loads(raft_status)
        term = int(raft_status.get("term", 0) or 0)
        return term if term > 0 else 1   # alive cluster always has term >= 1
    except Exception:
        return 1   # safe fallback — term > 0 guard will pass

def parse_metrics(text: str) -> dict[tuple[str, str], float]:
    """
    Parse Prometheus text format into {(metric_name, labels_str): value}.
    labels_str includes the surrounding braces, e.g. '{To="http://node2:2380"}'.
    Lines starting with '#' are skipped.
    """
    result = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        try:
            # Split off trailing value (and optional timestamp)
            parts = line.rsplit(None, 1)
            if len(parts) != 2:
                continue
            name_labels, value = parts
            value = float(value)
            m = re.match(r"^([^\{]+)(\{.*\})?$", name_labels)
            if not m:
                continue
            metric_name  = m.group(1).strip()
            labels_str   = m.group(2) or ""
            result[(metric_name, labels_str)] = value
        except Exception:
            pass
    return result

def get_scalar(metrics: dict, name: str) -> float | None:
    """Return value for a metric with no labels, or None if not found."""
    return metrics.get((name, ""), None)

def get_labeled(metrics: dict, name: str, label_fragment: str) -> float | None:
    """Return value for first metric whose labels string contains label_fragment."""
    for (n, labels), v in metrics.items():
        if n == name and label_fragment in labels:
            return v
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  Per-node state fetch
# ══════════════════════════════════════════════════════════════════════════════

def fetch_node_state(port: int) -> dict:
    """
    Returns:
        reachable  : bool
        raft_term  : int   — from /debug/vars raft.status (expvar)
        is_leader  : bool  — from etcd_server_is_leader Prometheus metric
        has_leader : bool  — from etcd_server_has_leader Prometheus metric
        metrics    : dict  (raw parsed metrics)

    Note: etcd_server_raft_term does NOT exist as a Prometheus metric in this
    version of etcd. The real term comes from /debug/vars.
    """
    raw = _fetch_raw_metrics(port)
    if not raw:
        return {"reachable": False, "raft_term": 0,
                "is_leader": False, "has_leader": False, "metrics": {}}
    m = parse_metrics(raw)
    return {
        "reachable":  True,
        "raft_term":  _fetch_raft_term(port),   # uses /debug/vars, not /metrics
        "is_leader":  (get_scalar(m, "etcd_server_is_leader") or 0) == 1.0,
        "has_leader": (get_scalar(m, "etcd_server_has_leader") or 0) == 1.0,
        "metrics":    m,
    }


# ══════════════════════════════════════════════════════════════════════════════
#  Cluster-level helpers
# ══════════════════════════════════════════════════════════════════════════════

def find_leader(
    node_indices: list[int],
    timeout: float = 15.0,
) -> tuple[int | None, int]:
    """
    Return (leader_node_idx, raft_term) or (None, 0) if none found.

    Retries for up to `timeout` seconds because etcd_server_is_leader is set
    asynchronously by the Raft ready-loop; it may lag briefly behind
    etcd_server_has_leader even when the cluster is healthy.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        for idx in node_indices:
            s = fetch_node_state(CLIENT_PORTS[idx])
            if s["is_leader"]:   # raft_term always > 0 for a live cluster
                return idx, s["raft_term"]
        time.sleep(0.5)
    return None, 0

def wait_cluster_healthy(
    node_indices: list[int],
    timeout: int,
    label: str = "",
) -> bool:
    """
    Block until all nodes report has_leader=True AND at least one node
    reports is_leader=True, or timeout is reached.
    """
    log(f"Waiting for all {len(node_indices)} nodes to be healthy{' ' + label if label else ''}…", indent=1)
    deadline = time.time() + timeout
    while time.time() < deadline:
        states = [fetch_node_state(CLIENT_PORTS[idx]) for idx in node_indices]
        healthy = sum(1 for s in states if s["has_leader"])
        leader_visible = any(s["is_leader"] for s in states)
        log(f"Healthy: {healthy}/{len(node_indices)}  leader visible: {leader_visible}", indent=2)
        if healthy == len(node_indices) and leader_visible:
            log("✓ Cluster healthy!", indent=1)
            return True
        time.sleep(3)
    log("✗ Cluster did not become healthy in time!", indent=1)
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  Thread-safe CSV event writer (per-node routing + merged file)
# ══════════════════════════════════════════════════════════════════════════════

class EventWriter:
    """
    Writes Raft events to two destinations simultaneously:
      1. A merged  results/<N>_nodes/round_<R>/events.csv
         (all nodes in timestamp order — used by analyze_election.py)
      2. Per-node  results/<N>_nodes/round_<R>/events_nodeN.csv
         (one file per node — used for per-node analysis charts)

    Row format: timestamp, event, node_id, term, failure_index
    """

    def __init__(self, round_dir: Path, node_indices: list[int]) -> None:
        self.round_dir = round_dir
        round_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        # Merged file
        self._merged_path = round_dir / "events.csv"
        self._merged_path.write_text("", encoding="utf-8")

        # Per-node files  {node_id_str -> Path}
        self._node_paths: dict[str, Path] = {}
        for idx in node_indices:
            p = round_dir / f"events_node{idx}.csv"
            p.write_text("", encoding="utf-8")
            self._node_paths[f"node{idx}"] = p

    def write(self, event: str, node_id: str, term: int = 0,
              failure_index: int = 0) -> None:
        """
        Write a row to the merged file and (if the node is known) to the
        per-node file. Extra nodes (e.g. a former leader restarted under a
        different term) are created lazily.
        """
        row = [_ts(), event, node_id, str(term), str(failure_index)]
        with self._lock:
            # Merged
            with open(self._merged_path, "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)
            # Per-node
            if node_id not in self._node_paths:
                p = self.round_dir / f"events_{node_id}.csv"
                p.write_text("", encoding="utf-8")
                self._node_paths[node_id] = p
            with open(self._node_paths[node_id], "a", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(row)


# ══════════════════════════════════════════════════════════════════════════════
#  Background cluster monitor
# ══════════════════════════════════════════════════════════════════════════════

class ClusterMonitor:
    """
    Runs in a background thread, polls /metrics every POLL_INTERVAL seconds.

    Emits events:
      • became candidate  — when raft_term increases on a follower
      • became leader     — when etcd_server_is_leader flips to 1
      • sent heartbeat to nodeN  — when peer byte count grows on the leader
    """

    def __init__(self, writer: "EventWriter", node_indices: list[int],
                 failure_index: int = 0) -> None:
        self._writer       = writer
        self._lock         = threading.Lock()
        self._indices      = list(node_indices)
        self._stop         = threading.Event()
        self._thread: threading.Thread | None = None
        self._failure_index = failure_index
        # Per-node previous state
        self._prev: dict[int, dict] = {
            idx: {"term": 0, "is_leader": False, "peer_bytes": {}}
            for idx in node_indices
        }

    def update_active_nodes(self, indices: list[int]) -> None:
        with self._lock:
            self._indices = list(indices)
            for idx in indices:
                if idx not in self._prev:
                    self._prev[idx] = {"term": 0, "is_leader": False, "peer_bytes": {}}

    def start(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    # ── internal ──────────────────────────────────────────────────────────────

    def _loop(self) -> None:
        while not self._stop.is_set():
            with self._lock:
                indices = list(self._indices)
            for idx in indices:
                try:
                    self._poll(idx, indices)
                except Exception:
                    pass
            self._stop.wait(POLL_INTERVAL)

    def _poll(self, idx: int, all_indices: list[int]) -> None:
        port  = CLIENT_PORTS[idx]
        state = fetch_node_state(port)
        prev  = self._prev[idx]

        if not state["reachable"]:
            return

        term      = state["raft_term"]
        is_leader = state["is_leader"]

        # ── Term change → election started
        if term > prev["term"] and term > 0:
            if not is_leader:
                self._writer.write("became candidate", f"node{idx}", term,
                                   self._failure_index)
            prev["term"] = term

        # ── Leadership change
        if is_leader and not prev["is_leader"]:
            self._writer.write("became leader", f"node{idx}", term,
                               self._failure_index)
            prev["is_leader"] = True
        elif not is_leader and prev["is_leader"]:
            prev["is_leader"] = False

        if term > prev["term"]:
            prev["term"] = term

        # ── Heartbeats (only the current leader sends them)
        if is_leader:
            for peer_idx in all_indices:
                if peer_idx == idx:
                    continue
                peer_url   = f"http://node{peer_idx}:2380"
                label_filt = f'"{peer_url}"'
                sent = get_labeled(
                    state["metrics"],
                    "etcd_network_peer_sent_bytes_total",
                    label_filt,
                )
                if sent is None:
                    continue
                prev_sent = prev["peer_bytes"].get(peer_idx, 0)
                if sent > prev_sent:
                    self._writer.write(
                        f"sent heartbeat to node{peer_idx}",
                        f"node{idx}",
                        term,
                        self._failure_index,
                    )
                    prev["peer_bytes"][peer_idx] = sent


# ══════════════════════════════════════════════════════════════════════════════
#  Single-round experiment  (FAILURES_PER_ROUND kill→elect→restart cycles)
# ══════════════════════════════════════════════════════════════════════════════

def run_round(
    cluster_size: int,
    round_num:    int,
    node_indices: list[int],
    writer:       "EventWriter",
) -> bool:
    """
    Execute one round consisting of FAILURES_PER_ROUND kill→re-elect→restart
    cycles in sequence.

    Returns True if at least one failure cycle completed successfully.
    """
    log(f"─── Round {round_num} ─────────────────────────────", indent=1)

    any_success  = False
    active_nodes = list(node_indices)   # shrinks by 0 net (restart restores node)

    for failure_idx in range(1, FAILURES_PER_ROUND + 1):
        log(f"  ── Failure {failure_idx}/{FAILURES_PER_ROUND} ──────────────", indent=1)

        # ── 1. Find leader ────────────────────────────────────────────────────
        leader_idx, current_term = find_leader(active_nodes)
        if leader_idx is None:
            log("⚠ No leader found — skipping this failure cycle.", indent=2)
            continue
        log(f"Leader: node{leader_idx}  (term {current_term})", indent=2)

        # ── 2. Start monitor + baseline ───────────────────────────────────────
        monitor = ClusterMonitor(writer, active_nodes, failure_index=failure_idx)
        monitor.start()
        writer.write("became leader", f"node{leader_idx}", current_term, failure_idx)
        time.sleep(PRE_KILL_STABILIZE)

        # ── 3. Kill the leader ────────────────────────────────────────────────
        cname        = container_name(cluster_size, leader_idx)
        failure_time = datetime.now()
        log(f"Killing leader: {cname}", indent=2)
        writer.write("leader failure", f"node{leader_idx}", current_term, failure_idx)

        if not stop_container(cname):
            log("Failed to stop container — aborting this cycle.", indent=2)
            monitor.stop()
            continue

        surviving = [i for i in active_nodes if i != leader_idx]
        monitor.update_active_nodes(surviving)

        # ── 4. Wait for new leader ────────────────────────────────────────────
        log(f"Waiting for new leader (timeout {ELECTION_TIMEOUT}s)…", indent=2)
        new_leader_idx  = None
        new_leader_term = 0
        deadline        = time.time() + ELECTION_TIMEOUT

        while time.time() < deadline:
            for idx in surviving:
                s = fetch_node_state(CLIENT_PORTS[idx])
                if s["is_leader"] and s["raft_term"] > current_term:
                    new_leader_idx  = idx
                    new_leader_term = s["raft_term"]
                    break
            if new_leader_idx:
                break
            time.sleep(POLL_INTERVAL)

        if new_leader_idx is None:
            log("✗ No new leader within timeout!", indent=2)
            monitor.stop()
            start_container(cname)   # try to restore cluster
            time.sleep(POST_RECOVER_SETTLE)
            continue

        elapsed = (datetime.now() - failure_time).total_seconds()
        log(
            f"✓ New leader: node{new_leader_idx}  "
            f"term {new_leader_term}  "
            f"({elapsed:.6f}s after failure)",
            indent=2,
        )
        time.sleep(POST_ELECT_SETTLE)

        # ── 5. Restart the killed node ────────────────────────────────────────
        log(f"Restarting node{leader_idx}…", indent=2)
        restart_time = datetime.now()
        writer.write("node started", f"node{leader_idx}", new_leader_term, failure_idx)
        ok = start_container(cname)

        # ── 6. Wait for recovery ──────────────────────────────────────────────
        if ok:
            monitor.update_active_nodes(active_nodes)
            log(f"Waiting for node{leader_idx} to rejoin (timeout {RECOVERY_TIMEOUT}s)…", indent=2)
            rec_deadline = time.time() + RECOVERY_TIMEOUT
            recovered    = False

            while time.time() < rec_deadline:
                s = fetch_node_state(CLIENT_PORTS[leader_idx])
                if s["reachable"] and s["has_leader"]:
                    rec_elapsed = (datetime.now() - restart_time).total_seconds()
                    log(f"✓ node{leader_idx} rejoined in {rec_elapsed:.6f}s", indent=2)
                    writer.write("append entries",    f"node{leader_idx}", new_leader_term, failure_idx)
                    writer.write("received heartbeat", f"node{leader_idx}", new_leader_term, failure_idx)
                    recovered = True
                    break
                time.sleep(0.5)

            if not recovered:
                log(f"⚠ node{leader_idx} did not rejoin within timeout.", indent=2)
        else:
            log(f"⚠ docker start {cname} failed.", indent=2)

        monitor.stop()
        any_success = True

        # Settle between failure cycles (except after the last one)
        if failure_idx < FAILURES_PER_ROUND:
            log(f"Settling {POST_RECOVER_SETTLE}s before next failure…", indent=2)
            time.sleep(POST_RECOVER_SETTLE)

    log(f"Round {round_num} complete — {FAILURES_PER_ROUND} failure cycles attempted.", indent=1)
    return any_success


# ══════════════════════════════════════════════════════════════════════════════
#  Full cluster experiment (all rounds for one cluster size)
# ══════════════════════════════════════════════════════════════════════════════

def run_cluster_experiment(cluster_size: int, num_rounds: int) -> None:
    compose_file = SCRIPT_DIR / f"docker-compose-{cluster_size}node.yml"
    project      = f"etcd{cluster_size}"
    node_indices = list(range(1, cluster_size + 1))
    cluster_dir  = RESULTS_DIR / f"{cluster_size}_nodes"
    cluster_dir.mkdir(parents=True, exist_ok=True)

    log(f"\n{'═' * 62}")
    log(f"  {cluster_size}-NODE etcd CLUSTER EXPERIMENT")
    log(f"{'═' * 62}")

    # ── Start cluster ──────────────────────────────────────────────────────
    log("Building image and starting cluster…", indent=1)
    log("(First build may take several minutes — compiling etcd from source)", indent=1)
    r = docker_compose(compose_file, "up", "-d", "--build", project=project)
    if r.returncode != 0:
        log(f"✗ docker compose up failed:\n{r.stderr}", indent=1)
        return

    log("Container start initiated. Waiting for initial grace period…", indent=1)
    time.sleep(15)

    if not wait_cluster_healthy(node_indices, CLUSTER_STARTUP_TIMEOUT, "(startup)"):
        log("✗ Cluster never became healthy. Aborting.", indent=1)
        docker_compose(compose_file, "down", "-v", project=project)
        return

    # ── Rounds ────────────────────────────────────────────────────────────
    for rnd in range(1, num_rounds + 1):
        log(f"\nPreparing Round {rnd}/{num_rounds}…", indent=1)

        # Bring up any containers that were stopped in the previous round
        docker_compose(compose_file, "start", project=project)
        time.sleep(5)

        if not wait_cluster_healthy(node_indices, INTER_ROUND_STARTUP_TIMEOUT,
                                    f"(before round {rnd})"):
            log(f"✗ Cluster not healthy before round {rnd} — skipping.", indent=1)
            continue

        # Per-round EventWriter — creates merged + per-node files
        round_dir = cluster_dir / f"round_{rnd}"
        writer    = EventWriter(round_dir, node_indices)

        ok = run_round(cluster_size, rnd, node_indices, writer)
        log(f"Round {rnd} events → {round_dir / 'events.csv'}", indent=1)

        if not ok:
            log(f"Round {rnd} encountered errors.", indent=1)

        if rnd < num_rounds:
            log(f"Pausing {INTER_ROUND_PAUSE}s before next round…", indent=1)
            time.sleep(INTER_ROUND_PAUSE)

    # ── Tear down ──────────────────────────────────────────────────────────
    log("\nTearing down cluster and removing volumes…", indent=1)
    docker_compose(compose_file, "down", "-v", project=project)
    log(f"✓ {cluster_size}-node experiment complete.", indent=1)


# ══════════════════════════════════════════════════════════════════════════════
#  Post-processing
# ══════════════════════════════════════════════════════════════════════════════

def run_analysis(cluster_sizes: list[int], num_rounds: int) -> None:
    log(f"\n{'═' * 62}")
    log("  Election Analysis")
    log(f"{'═' * 62}")
    analyzer = SCRIPT_DIR / "analyze_election.py"

    for cs in cluster_sizes:
        for rnd in range(1, num_rounds + 1):
            round_dir = RESULTS_DIR / f"{cs}_nodes" / f"round_{rnd}"
            events    = round_dir / "events.csv"
            report    = round_dir / "election_report.csv"
            if not events.exists():
                log(f"⚠ Missing: {events}", indent=1)
                continue
            log(f"Analyzing {cs}-node round {rnd}…", indent=1)
            r = subprocess.run(
                [sys.executable, str(analyzer),
                 str(round_dir), str(report), str(cs)],
                capture_output=True, text=True,
            )
            if r.returncode:
                log(f"⚠ analyze_election error:\n{r.stderr.strip()}", indent=2)
            else:
                log(f"✓ {report.name}", indent=2)


def run_chart_generation() -> None:
    log(f"\n{'═' * 62}")
    log("  Chart Generation")
    log(f"{'═' * 62}")
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)
    generator = SCRIPT_DIR / "generate_charts.py"
    r = subprocess.run(
        [sys.executable, str(generator), str(RESULTS_DIR), str(CHARTS_DIR)],
        capture_output=True, text=True,
    )
    if r.returncode:
        log(f"⚠ generate_charts error:\n{r.stderr}", indent=1)
    else:
        for line in r.stdout.splitlines():
            log(line, indent=1)


# ══════════════════════════════════════════════════════════════════════════════
#  CLI entry point
# ══════════════════════════════════════════════════════════════════════════════

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="etcd Raft Cluster Performance Benchmark",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--cluster-sizes", nargs="+", type=int, default=[3, 5, 7],
        metavar="N",
        help="Cluster sizes to test (default: 3 5 7)",
    )
    p.add_argument(
        "--rounds", type=int, default=4,
        help="Number of leader-failure rounds per cluster (default: 4)",
    )
    p.add_argument(
        "--analyze-only", action="store_true",
        help="Skip cluster experiments; only run analysis + charts",
    )
    p.add_argument(
        "--charts-only", action="store_true",
        help="Skip experiments and analysis; only regenerate charts",
    )
    return p.parse_args()


def main() -> None:
    args   = _parse_args()
    sizes  = args.cluster_sizes
    rounds = args.rounds

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    CHARTS_DIR.mkdir(parents=True, exist_ok=True)

    log("═" * 62)
    log("  etcd Raft Cluster Performance Benchmark")
    log("═" * 62)
    log(f"  Cluster sizes      : {sizes}")
    log(f"  Rounds/cluster     : {rounds}")
    log(f"  Failures/round     : {FAILURES_PER_ROUND}")
    log(f"  Results dir        : {RESULTS_DIR}")
    log(f"  Charts dir         : {CHARTS_DIR}")
    log("═" * 62)

    if not (args.analyze_only or args.charts_only):
        for cs in sizes:
            try:
                run_cluster_experiment(cs, rounds)
            except KeyboardInterrupt:
                log("\nInterrupted by user — stopping experiments.")
                break
            except Exception as exc:
                log(f"ERROR in {cs}-node experiment: {exc}")
                import traceback
                traceback.print_exc()

    if not args.charts_only:
        run_analysis(sizes, rounds)

    run_chart_generation()

    log("\n" + "═" * 62)
    log("  BENCHMARK COMPLETE")
    log(f"  CSVs   → {RESULTS_DIR}")
    log(f"  Charts → {CHARTS_DIR}")
    log("═" * 62)


if __name__ == "__main__":
    main()
