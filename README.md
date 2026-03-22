# slurmwatch

Lightweight CLI tool for monitoring Slurm cluster jobs. Designed to run on login nodes with minimal resource usage.

- Real-time TUI dashboard (curses-based, configurable refresh)
- Historical data collection to SQLite for trend analysis
- Per-user job breakdowns, queue wait time patterns, job efficiency reports
- Zero external dependencies (Python stdlib only)
- OSC cluster support with GPU efficiency via `osc-seff` / `gpu-seff`

## Install

```bash
pip install -e .
```

Requires Python 3.10+.

## Quick Start

```bash
# Live dashboard (refreshes every 30s, press q to quit)
slurmwatch dashboard

# Start collecting historical data (one-shot, good for cron)
slurmwatch collect

# Or run as a daemon (every 5 minutes)
slurmwatch collect --daemon --interval 300

# Per-user summary for the last 7 days
slurmwatch users --since 7d

# Queue wait time analysis by hour of day
slurmwatch waits --by-hour --since 7d

# Job efficiency report (low efficiency jobs)
slurmwatch efficiency --low 50 --since 7d

# Single job efficiency
slurmwatch efficiency --job 12345
```

## Commands

| Command | Description |
|---------|-------------|
| `dashboard` | TUI dashboard (`-r` refresh interval, `--user`, `--partition` filters) |
| `collect` | Snapshot cluster state to SQLite (`--daemon`, `--interval`, `--retention`) |
| `jobs` | List jobs (`--user`, `--state`, `--partition`, `--since`, `--sort`) |
| `users` | Per-user summary (`--since`, `--sort`, `--top`) |
| `waits` | Queue wait time stats (`--by-hour`, `--by-size`, `--partition`) |
| `efficiency` | Job efficiency report (`--job`, `--user`, `--low`, `--gpu`) |
| `config` | Show or set config (`config show`, `config set general.osc true`) |
| `db` | Database management (`db info`, `db prune`, `db vacuum`) |

## OSC Clusters

On OSC clusters (ascend, cardinal), enable GPU efficiency reporting:

```bash
slurmwatch config set general.osc true

# Now efficiency reports include GPU metrics
slurmwatch efficiency --job 12345

# Detailed per-GPU breakdown
slurmwatch efficiency --job 12345 --gpu
```

## Config

Settings are stored in `~/.slurmwatch/config.ini`. View with `slurmwatch config show`.

| Key | Default | Description |
|-----|---------|-------------|
| `general.osc` | `false` | Enable OSC-specific tools (osc-seff, gpu-seff) |
| `general.db_path` | (empty) | SQLite path (default: `~/.slurmwatch/data.db`) |
| `general.refresh_interval` | `30` | Dashboard refresh in seconds |
| `general.retention_days` | `30` | Days to keep historical data |
