# slurmmon-cli User Guide

## Installation

```bash
# Basic install (CLI subcommands only, zero dependencies)
pip install -e .

# With interactive TUI (requires textual)
pip install -e ".[tui]"
```

## File Locations

slurmmon-cli follows the [XDG Base Directory Specification](https://specifications.freedesktop.org/basedir-spec/latest/):

| File | Default Path | Environment Override |
|------|-------------|---------------------|
| Config | `~/.config/slurmmon-cli/config.ini` | `$XDG_CONFIG_HOME/slurmmon-cli/config.ini` |
| Database | `~/.local/share/slurmmon-cli/data.db` | `$XDG_DATA_HOME/slurmmon-cli/data.db` |

Both directories are created automatically on first use.

You can override paths per-invocation:
```bash
slurmmon-cli --config /path/to/config.ini --db /path/to/data.db <command>
```

## Interactive TUI

Launch with no arguments (requires `textual` installed):
```bash
slurmmon-cli
```

### Screen Navigation

| Key | Screen | Description |
|-----|--------|-------------|
| `m` | Monitor | Real-time cluster dashboard (default) |
| `x` | Explorer | GPU/CPU usage analysis with tabbed views |
| `s` | Settings | Configuration UI (overlay, Escape to close) |
| `q` | - | Quit |

### Monitor Screen

Shows real-time cluster state with auto-refresh (default 30s):
- Cluster summary: node counts, CPU utilization bar
- Partition table
- Running jobs table (sortable)
- Pending jobs table

Keys: `r` refresh, `u` user filter, `p` partition filter

### Explorer Screen

Tabbed analysis views:
- **GPU Users** - top users by GPU-hours (from sshare data)
- **CPU Users** - top users by CPU-hours
- **Accounts** - top accounts by GPU-hours
- **Nodes** - color-coded heatmap of node utilization (green/yellow/red)
- **GPU Chart** - visual bar chart of GPU usage

Key: `r` refresh current tab

### Settings Screen

Toggle OSC mode, adjust refresh/retention intervals, view database info. Changes saved on Escape.

## CLI Subcommands

All subcommands work without the TUI dependency.

### Data Collection

```bash
# One-shot snapshot (good for cron)
slurmmon-cli collect

# Continuous daemon
slurmmon-cli collect --daemon --interval 300

# Cron example: collect every 5 minutes
*/5 * * * * cd /path/to/project && python -m slurmmon_cli collect
```

The collector gathers:
- Job queue (squeue) - all users' running/pending jobs
- Cluster info (sinfo) - partition and node state
- Completed jobs (sacct) - your own job history
- User usage (sshare) - all users' aggregate resource usage (every 30 min)

### Querying

```bash
# Per-user summary
slurmmon-cli users --since 7d --top 10

# Job listing
slurmmon-cli jobs --user alice --state COMPLETED --since 24h

# Queue wait time analysis
slurmmon-cli waits --by-hour --since 7d
slurmmon-cli waits --by-size --partition longgpu

# Job efficiency (your own jobs)
slurmmon-cli efficiency --since 7d --low 50
slurmmon-cli efficiency --job 12345

# GPU usage explorer
slurmmon-cli explore --by gpu --top 20
slurmmon-cli explore --by account
slurmmon-cli explore --by nodes --top 30
slurmmon-cli explore --by requests
slurmmon-cli explore --by delta --hours 24
```

### Configuration

```bash
# View all settings
slurmmon-cli config show

# Enable OSC tools (osc-seff, gpu-seff)
slurmmon-cli config set general.osc true

# Adjust settings
slurmmon-cli config set general.refresh_interval 60
slurmmon-cli config set general.retention_days 14
```

### Database Management

```bash
slurmmon-cli db info      # Show DB stats
slurmmon-cli db prune     # Delete data older than 30 days
slurmmon-cli db vacuum    # Compact database file
```

## OSC Cluster Features

On OSC clusters (ascend, cardinal), enable GPU efficiency reporting:

```bash
slurmmon-cli config set general.osc true
```

This enables:
- `osc-seff` for combined CPU+GPU efficiency per job
- `gpu-seff --json` for detailed per-GPU breakdown
- `slurmmon-cli efficiency --job <id> --gpu` for GPU detail view
