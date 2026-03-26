# Changelog

## v0.1.0

### Features

**TUI Dashboard (Textual-based)**
- Multi-screen TUI with Monitor, Explore, Efficiency, and Settings screens
- Auto-collects cluster data on startup (squeue, sinfo, sacct, sshare)
- Status banner and toast notification when collection completes
- Cluster auto-detection via sinfo with scontrol fallback

**Monitor Screen**
- Real-time cluster summary: node/CPU allocation, running/pending job counts
- Partition table with node states and time limits
- Running and pending job tables (top 100 each, sorted by elapsed/wait time)
- User and partition filters (U/P keys)
- Configurable refresh interval

**Explore Screen**
- GPU Users tab: top users ranked by GPU-hours from sshare, with fairshare, active job counts, and node breakdown (full/partial)
- CPU Users tab: top users by CPU-hours
- Accounts tab: top accounts by GPU-hours with user count and job/node breakdown
- Nodes tab: color-coded node utilization heatmap grouped by partition, with sort/view/partition cycling and per-node detail on Enter
- GPU Chart tab: switchable bar chart (GPU-hours, all nodes, full nodes) with keyboard navigation

**Efficiency Screen (GPU-focused)**
- GPU Jobs tab: user's running + completed GPU jobs with CPU/memory/walltime efficiency percentages
- GPU Queue tab: GPU vs CPU-only wait time comparison, wait time by GPU count and by partition, with scheduling policy disclaimer
- GPU Activity tab: live per-partition GPU allocation, top GPU consumers (DataTable with Enter drill-down), pending demand summary, GPU allocation trend sparkline
- GPU Waste tab: GPU jobs with low CPU efficiency (<50%), walltime waste (<30% of limit), underutilized GPU nodes, full-node filter toggle (F key)

**Detail Views**
- User detail modal: running/pending jobs with ACCOUNT and CLUSTER columns, multi-project-code support
- Account detail modal: all users and jobs under an account
- Node detail modal: node stats (CPU/GPU/memory) and running jobs
- All detail modals: responsive width (90% with min/max bounds), auto-focused DataTable
- Grafana URL generation: press G to copy OSC Grafana node-metrics URL to clipboard (OSC 52)

**CLI Commands**
- `explore` / `x`: CLI GPU and resource usage explorer with 6 ranking modes (gpu, cpu, account, requests, delta, nodes)
- `collect`: one-shot or daemon data collection with configurable interval and retention
- `jobs`: per-user or global job listing with state/partition/time filters
- `users`: per-user summary with sorting and top-N
- `waits`: queue wait time analysis by hour-of-day or job size
- `efficiency`: job efficiency reports with optional GPU breakdown (osc-seff/gpu-seff)
- `config`: show/set INI-based configuration
- `db`: database info, pruning, and vacuum

**Data Collection and Storage**
- SQLite database with WAL mode for concurrent reads during collection
- Incremental job history via sacct with starttime tracking
- sshare collection with per-GPU-type usage breakdown (gated by configurable interval)
- Per-job cluster tagging for multi-cluster environments
- Cluster snapshot history with GPU allocation counts
- Schema migrations (v1 through v5) for safe upgrades

### Quality of Life

**Navigation**
- Grouped footer: `Nav [M]onitor [X]plore [E]fficiency  Action [R]efresh  [Tab] switch tab  [?] Settings  [Q] Quit`
- Tab hints on every tabbed screen showing available keys (Enter, Esc, per-tab actions)
- Left/Right arrow keys for horizontal scrolling on wide tables
- Cursor position preserved when returning from detail modals via Esc
- Auto-focused DataTable in detail modals (arrow keys work immediately)

**Cluster Compatibility**
- Robust cluster name detection with scontrol fallback (handles empty sinfo --json cluster field)
- Stale cluster name migration on startup (fixes data stored as empty or "unknown")
- Forced sshare collection on TUI startup for immediate explorer data
- Multi-cluster database: CLUSTER column on jobs and user_usage tables
- Rich markup escaping for key hints (brackets display literally)

**OSC Integration**
- osc-seff: GPU efficiency, GPU memory efficiency, GPU utilization
- gpu-seff: detailed per-GPU JSON breakdown
- Grafana URL builder: generates cluster-metrics dashboard URLs with var-host per node (var-cluster=All for multi-cluster)

**Configuration**
- XDG Base Directory compliant (config + data paths)
- INI-based config with sensible defaults
- `sshare_interval` config for tuning fairshare collection frequency
- `general.osc` flag to enable OSC-specific tools
