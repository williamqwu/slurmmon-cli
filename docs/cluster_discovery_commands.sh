#!/bin/bash
# Run these commands on the Slurm login node and share the output.
# You can run the whole script: bash cluster_discovery_commands.sh

echo "=== 1. Python version ==="
python3 --version

echo ""
echo "=== 2. Slurm version ==="
squeue --version

echo ""
echo "=== 3. squeue JSON support (first 20 lines) ==="
squeue --json 2>&1 | head -20

echo ""
echo "=== 4. sacct JSON support (first 30 lines) ==="
sacct --json -S now-1hour 2>&1 | head -30

echo ""
echo "=== 5. seff availability ==="
which seff 2>&1 && seff --help 2>&1 | head -5

echo ""
echo "=== 6. sinfo JSON (first 20 lines) ==="
sinfo --json 2>&1 | head -20

echo ""
echo "=== 7. Partitions overview ==="
sinfo -s

echo ""
echo "=== 8. PrivateData config ==="
scontrol show config | grep PrivateData

echo ""
echo "=== 9. Available disk space ==="
df -h ~

echo ""
echo "=== 10. Terminal capabilities ==="
echo "TERM=$TERM"
locale | grep LC_CTYPE
