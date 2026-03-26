# slurmmon-cli

Lightweight CLI tool for monitoring Slurm cluster jobs. Designed to run on login nodes with minimal resource usage.

- Real-time TUI dashboard with multi-screen navigation (Textual-based)
- GPU-focused analysis: usage rankings, queue wait times, activity, waste detection
- Historical data collection to SQLite for trend analysis
- Per-user job breakdowns, fairshare tracking, node utilization heatmap
- OSC cluster support with GPU efficiency via `osc-seff` / `gpu-seff`
- Grafana URL generation for node metrics

## Install

```bash
pip install -e ".[tui]"
```

Requires Python 3.10+. The `[tui]` extra installs Textual for the interactive dashboard.

## Quick Start

```bash
# Launch interactive TUI (default command)
slurmmon-cli

# Or explicitly
slurmmon-cli dashboard

# Collect historical data (one-shot, good for cron)
slurmmon-cli collect

# Run collector as daemon (every 5 minutes)
slurmmon-cli collect --daemon --interval 300

# CLI GPU usage explorer
slurmmon-cli explore --by gpu --top 20
```

## TUI Dashboard

The dashboard auto-collects data on startup and provides four screens:

| Key | Screen | Description |
|-----|--------|-------------|
| `M` | Monitor | Real-time cluster overview: partitions, running/pending jobs |
| `X` | Explore | GPU/CPU usage rankings, accounts, node heatmap, GPU chart |
| `E` | Efficiency | GPU jobs, queue wait analysis, GPU activity, waste detection |
| `?` | Settings | View and modify configuration |

Navigation: `Tab` switches between tabs within a screen. `Enter` opens detail views. `Esc` closes detail views. `Left`/`Right` arrows scroll wide tables horizontally. `Q` quits.

### Explore Screen Tabs

- **GPU Users** / **CPU Users** - Top users ranked by GPU/CPU-hours from sshare, with fairshare priority
- **Accounts** - Top accounts by resource usage
- **Nodes** - Color-coded node utilization heatmap (`O` sort, `V` view mode, `P` partition filter)
- **GPU Chart** - Switchable bar chart of GPU usage metrics

### Efficiency Screen Tabs

- **GPU Jobs** - Your running and completed GPU jobs with CPU/memory/walltime efficiency
- **GPU Queue** - Wait time comparison (GPU vs CPU-only), breakdown by GPU count and partition
- **GPU Activity** - Live per-partition GPU allocation, top consumers, pending demand
- **GPU Waste** - Low CPU efficiency on GPU jobs, walltime waste, underutilized GPU nodes (`F` toggles full-node filter)

### Detail Views

Press `Enter` on any user, account, or node row to open a detail modal showing active jobs. In user/account detail views, press `G` to copy a Grafana node-metrics URL to the clipboard.

## CLI Commands

| Command | Description |
|---------|-------------|
| `dashboard` / `d` | TUI dashboard (`-r` refresh, `--user`, `--partition`, `--from-db`) |
| `collect` | Snapshot cluster state (`--daemon`, `--interval`, `--retention`) |
| `explore` / `x` | GPU/resource explorer (`--by gpu\|cpu\|account\|requests\|delta\|nodes`) |
| `jobs` | List jobs (`--user`, `--state`, `--partition`, `--since`, `--sort`) |
| `users` | Per-user summary (`--since`, `--sort`, `--top`) |
| `waits` | Queue wait times (`--by-hour`, `--by-size`, `--partition`) |
| `efficiency` | Job efficiency (`--job`, `--user`, `--low`, `--gpu`) |
| `config` | Configuration (`config show`, `config set section.key value`) |
| `db` | Database management (`db info`, `db prune`, `db vacuum`) |

## OSC Clusters

On OSC clusters (Ascend, Cardinal), enable GPU efficiency reporting:

```bash
slurmmon-cli config set general.osc true

# Job efficiency with GPU metrics
slurmmon-cli efficiency --job 12345

# Detailed per-GPU breakdown
slurmmon-cli efficiency --job 12345 --gpu
```

## Config

File locations follow the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/):

- Config: `$XDG_CONFIG_HOME/slurmmon-cli/config.ini` (default: `~/.config/slurmmon-cli/config.ini`)
- Data: `$XDG_DATA_HOME/slurmmon-cli/data.db` (default: `~/.local/share/slurmmon-cli/data.db`)

View with `slurmmon-cli config show`.

| Key | Default | Description |
|-----|---------|-------------|
| `general.osc` | `false` | Enable OSC-specific tools (osc-seff, gpu-seff) |
| `general.db_path` | (empty) | SQLite path (default: XDG data dir) |
| `general.refresh_interval` | `30` | Dashboard refresh in seconds |
| `general.retention_days` | `30` | Days to keep historical data |
| `general.sshare_interval` | `1800` | Seconds between sshare collections |
