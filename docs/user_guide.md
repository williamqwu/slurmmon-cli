# slurmmon-cli User Guide

## Installation

```bash
# Basic install (CLI subcommands only, zero dependencies)
pip install -e .

# With interactive TUI (requires textual)
pip install -e ".[tui]"
```

### Multi-Cluster Setup (e.g., OSC ascend + cardinal)

If multiple clusters share a home directory via NFS but have different Python
installations (e.g., OSC's ascend and cardinal), `pip install -e .` hardcodes
the installing cluster's Python path in the `slurmmon-cli` shebang. The entry
point script will fail on the other cluster.

**Recommended fix**: use a shell alias instead of the pip entry point. Add to
your `~/.bashrc`:

```bash
alias slurmmon-cli='python -m slurmmon_cli'
```

This uses whichever Python is active on the current cluster. The pip-installed
packages in `~/.local/lib/python3.10/site-packages/` are shared and compatible
since both clusters use Python 3.10.

Alternatively, create a wrapper script at `~/bin/slurmmon-cli`:

```bash
#!/bin/bash
exec python -m slurmmon_cli "$@"
```

```bash
chmod +x ~/bin/slurmmon-cli
# Ensure ~/bin is in your PATH (add to ~/.bashrc if needed)
```

### Uninstalling

```bash
pip uninstall slurmmon-cli
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

If migrating from the old `~/.slurmmon-cli/` layout:
```bash
mkdir -p ~/.config/slurmmon-cli ~/.local/share/slurmmon-cli
mv ~/.slurmmon-cli/config.ini ~/.config/slurmmon-cli/ 2>/dev/null
mv ~/.slurmmon-cli/data.db* ~/.local/share/slurmmon-cli/ 2>/dev/null
rmdir ~/.slurmmon-cli 2>/dev/null
```

## Interactive TUI

Launch with no arguments (requires `textual` installed):
```bash
slurmmon-cli
```

If you get `The interactive dashboard requires 'textual'`, install with:
```bash
pip install -e ".[tui]"
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
- **GPU Users** - top users by GPU-hours, active GPU jobs (R/P), nodes used
- **CPU Users** - top users by CPU-hours
- **Accounts** - top accounts by GPU-hours
- **Nodes** - color-coded heatmap of node utilization
  - Green: >= 80% load ratio
  - Yellow: 50-80%
  - Red: < 50% (underutilized)
  - Gray: idle
  - Bold/`*`: exclusive-use (single user with >= 90% CPUs allocated)
- **GPU Chart** - visual bar chart of GPU usage

Key: `r` refresh current tab

### Settings Screen

Toggle OSC mode, adjust refresh/retention intervals, view database info. Changes saved on Escape.

### Resource Usage on Login Nodes

The TUI uses ~50-80 MB memory and near-zero CPU when idle between refreshes.
For a busy login node:
- **Interactive sessions**: the TUI is fine for checking status and closing
- **Persistent monitoring**: use `slurmmon-cli collect --daemon` instead (no UI overhead), then query with CLI subcommands
- **Increase refresh interval** if concerned: `slurmmon-cli config set general.refresh_interval 60`

The CLI subcommands (explore, users, jobs, etc.) are very lightweight - they run, print, and exit.

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
- Completed jobs (sacct) - your own job history only (Slurm restriction)
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

# Job efficiency (your own jobs only)
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

## Data Visibility Limitations

Due to Slurm permissions on OSC clusters:
- **squeue**: shows all users' running/pending jobs (full visibility)
- **sacct**: shows only your own completed jobs (efficiency data limited to your jobs)
- **sshare**: shows all users' aggregate CPU/GPU usage (GPU-hours, fairshare)
- **scontrol show node**: shows per-node CPU load and memory (all nodes)

This means per-job efficiency analysis (`slurmmon-cli efficiency`) only covers
your own jobs. For cross-user analysis, use `slurmmon-cli explore` which
combines squeue (who's requesting what) and sshare (who's consumed the most
GPU-hours) data. The `explore --by nodes` view uses per-node CPU load to detect
underutilized nodes regardless of which user owns them.
