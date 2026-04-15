# etcd Raft Cluster Performance Benchmark

Benchmarks **real etcd** (not the raft-example) across 3-node, 5-node, and 7-node clusters.  
Each cluster runs **4 rounds** of leader-failure injection and measures Raft timing metrics.

---

## Folder Structure

```
benchmark/
├── Dockerfile                    ← builds real etcd binary from source
├── docker-compose-3node.yml      ← 3-node cluster definition
├── docker-compose-5node.yml      ← 5-node cluster definition
├── docker-compose-7node.yml      ← 7-node cluster definition
│
├── run_experiment.py             ← MAIN SCRIPT (orchestrates everything)
├── analyze_election.py           ← parses events.csv → election_report.csv
├── generate_charts.py            ← reads reports → produces 5 PNG charts
├── requirements.txt              ← pip dependencies
│
└── results/                      ← auto-created when you run the experiment
    ├── 3_nodes/
    │   ├── round_1/
    │   │   ├── events.csv            raw Raft events (timestamp, event, node, term)
    │   │   └── election_report.csv   analyzed report for this round
    │   ├── round_2/ … round_4/
    ├── 5_nodes/ …
    ├── 7_nodes/ …
    └── charts/
        ├── cluster_performance.png        avg metrics vs cluster size
        ├── performance_cdf.png            CDF by cluster size
        ├── node_recovery_performance.png  recovery time vs downtime
        ├── metrics_boxplot.png            box-plot distribution per metric
        └── cluster_bar_comparison.png     grouped bar chart overview
```

---

## Prerequisites

| Tool | Version |
|---|---|
| Docker Desktop | ≥ 4.x (with Compose v2 built-in) |
| Python | ≥ 3.10 |

> **Note:** The first `docker compose up --build` compiles etcd from source inside  
> Alpine + Go — expect **5–15 minutes** for the initial image build.  
> Subsequent runs reuse the cached image layer.

---

## Quick Start

### 1. Install Python dependencies

```powershell
cd benchmark
pip install -r requirements.txt
```

### 2. Run the full benchmark (all cluster sizes, 4 rounds each)

```powershell
python run_experiment.py
```

### 3. Results

| Output | Location |
|---|---|
| Raw events | `results/{N}_nodes/round_{R}/events.csv` |
| Per-round report | `results/{N}_nodes/round_{R}/election_report.csv` |
| PNG charts | `results/charts/*.png` |

---

## CLI Options

```
python run_experiment.py [options]

  --cluster-sizes  3 5 7    Cluster sizes to test (default: 3 5 7)
  --rounds         4        Rounds per cluster    (default: 4)
  --analyze-only            Skip experiments; rerun analysis + charts only
  --charts-only             Skip experiments + analysis; regenerate charts only
```

### Examples

```powershell
# Only test 3-node and 5-node clusters, 2 rounds each
python run_experiment.py --cluster-sizes 3 5 --rounds 2

# Re-generate all charts from existing CSVs (no Docker needed)
python run_experiment.py --charts-only

# Re-run analysis and charts without re-running the Docker experiments
python run_experiment.py --analyze-only
```

---

## What Each Script Does

### `run_experiment.py` — Orchestrator

For each cluster size [3, 5, 7]:
1. **Builds** the etcd Docker image (first run only)
2. **Starts** the cluster with `docker compose up -d --build`
3. **Waits** up to 120 s for all nodes to report `has_leader=true`
4. For each round (1–4):
   - Identifies the current **Raft leader** via `/metrics` endpoint
   - **Kills** the leader container (`docker stop`)
   - Polls surviving nodes every 200 ms until a new leader is elected
   - **Restarts** the killed container (`docker start`)
   - Waits until the node rejoins (polls `/metrics` for `has_leader`)
   - Writes `events.csv` with timestamped Raft events
5. **Tears down** the cluster (`docker compose down -v`)
6. Runs `analyze_election.py` on every round's `events.csv`
7. Runs `generate_charts.py` to produce all PNG graphs

### `analyze_election.py` — Event Analyzer

Reads a single `events.csv` and writes `election_report.csv` with:

| Column | Meaning |
|---|---|
| Term Number | Raft term in which the election occurred |
| Leader Failure Detection Duration (s) | Time from failure to first candidate |
| Who Initiated Election | Node that became candidate first |
| Who Became Leader | Node that won the election |
| Leader Election Duration (s) | First candidate → new leader |
| Out of Service Time (s) | Failure → new leader (total downtime) |
| Average Heartbeat Interval (s) | Mean HB interval observed from leader |
| Rejoining Nodes & Recovery Time (s) | Per-node downtime and sync duration |

### `generate_charts.py` — Chart Generator

Produces **5 PNG charts** from all `election_report.csv` files:

1. **`cluster_performance.png`** — Line chart of avg Failure Detection / Election Duration / OOS vs cluster size
2. **`performance_cdf.png`** — CDF of all three metrics, one line per (cluster size, metric)
3. **`node_recovery_performance.png`** — Scatter + line of recovery time vs node downtime
4. **`metrics_boxplot.png`** — Box plots showing distribution across rounds
5. **`cluster_bar_comparison.png`** — Grouped bar chart for quick side-by-side comparison

---

## Port Mapping

| Node | Client Port | Peer Port |
|---|---|---|
| node1 | 12379 | 12380 |
| node2 | 22379 | 22380 |
| node3 | 32379 | 32380 |
| node4 | 42379 | 42380 |
| node5 | 52379 | 52380 |
| node6 | 62379 | 62380 |
| node7 | 72379 | 72380 |

Clusters run **sequentially** so there are no port conflicts.

---

## etcd Configuration

| Parameter | Value |
|---|---|
| `ETCD_HEARTBEAT_INTERVAL` | 100 ms |
| `ETCD_ELECTION_TIMEOUT` | 1000 ms |
| Data persistence | Named Docker volume (cleared between cluster sizes) |

---

## Troubleshooting

**Build fails with Go errors**  
→ Ensure you run from inside the `benchmark/` directory and that the parent directory is the etcd repo root.

**Cluster stays unhealthy after startup**  
→ Increase `CLUSTER_STARTUP_TIMEOUT` at the top of `run_experiment.py` (default: 120 s).

**No new leader elected within timeout**  
→ Increase `ELECTION_TIMEOUT` in `run_experiment.py` (default: 60 s). This can happen on slow machines.

**`docker compose` not found**  
→ Make sure you have Docker Desktop with Compose v2. Try `docker compose version` to verify.

**Charts show no data**  
→ Run `python run_experiment.py --analyze-only` first to regenerate all `election_report.csv` files, then `--charts-only`.



<!-- Run this in powershell: -->
$env:PYTHONIOENCODING="utf-8" python run_experiment.py
