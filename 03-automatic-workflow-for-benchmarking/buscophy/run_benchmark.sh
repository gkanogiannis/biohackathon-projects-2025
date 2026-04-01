#!/bin/bash
#
# run_benchmark.sh - Run the BuscoPhy snakemake pipeline and measure per-step timing.
#
# Usage:
#   conda activate snakemake_9
#   bash run_benchmark.sh [CORES] [EXTRA_SNAKEMAKE_ARGS...]
#
# Output:
#   results/benchmark/           - Snakemake benchmark files (per-job: wall time, CPU, memory)
#   results/benchmark/timing.tsv - Aggregated per-rule timing summary
#   snakemake_run.log            - Full pipeline log with timestamps
#
# Example:
#   bash run_benchmark.sh 10
#   bash run_benchmark.sh 24 --use-singularity

set -euo pipefail

CORES=${1:-10}
shift 2>/dev/null || true
EXTRA_ARGS="$*"

LOGFILE="snakemake_run.log"
TIMING_DIR="results/benchmark"
TIMING_SUMMARY="${TIMING_DIR}/timing.tsv"

mkdir -p "${TIMING_DIR}"

echo "============================================="
echo " BuscoPhy Benchmark Run"
echo " Cores: ${CORES}"
echo " Start: $(date '+%Y-%m-%d %H:%M:%S')"
echo "============================================="

PIPELINE_START=$(date +%s)

# Run snakemake with timestamps on each log line
snakemake \
    --use-conda \
    --cores "${CORES}" \
    --resources write_slots=1 \
    --printshellcmds \
    ${EXTRA_ARGS} \
    2>&1 | while IFS= read -r line; do
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${line}"
    done | tee "${LOGFILE}"

PIPELINE_END=$(date +%s)
PIPELINE_ELAPSED=$((PIPELINE_END - PIPELINE_START))

echo ""
echo "============================================="
echo " Pipeline completed in ${PIPELINE_ELAPSED}s ($(date -d@${PIPELINE_ELAPSED} -u +%H:%M:%S 2>/dev/null || printf '%02d:%02d:%02d' $((PIPELINE_ELAPSED/3600)) $(((PIPELINE_ELAPSED%3600)/60)) $((PIPELINE_ELAPSED%60))))"
echo "============================================="

# Parse timestamps from log to compute per-rule wall-clock times
echo ""
echo "=== Per-rule timing (from log timestamps) ==="
echo -e "rule\tjob_id\tstart\tend\tduration_sec" > "${TIMING_SUMMARY}"

# Extract rule start and finish events from the log
grep -E "^\[.+\] (localrule|rule) " "${LOGFILE}" | while read -r line; do
    ts=$(echo "$line" | grep -oP '^\[\K[0-9-]+ [0-9:]+')
    rule=$(echo "$line" | grep -oP '(localrule|rule) \K\w+')
    echo "START ${ts} ${rule}"
done > "${TIMING_DIR}/_starts.tmp"

grep -E "^\[.+\] Finished jobid:" "${LOGFILE}" | while read -r line; do
    ts=$(echo "$line" | grep -oP '^\[\K[0-9-]+ [0-9:]+')
    jobid=$(echo "$line" | grep -oP 'jobid: \K\d+')
    rule=$(echo "$line" | grep -oP 'Rule: \K\w+')
    echo "END ${ts} ${rule} ${jobid}"
done > "${TIMING_DIR}/_ends.tmp"

# Match starts and ends, compute durations
python3 - "${TIMING_DIR}/_starts.tmp" "${TIMING_DIR}/_ends.tmp" "${TIMING_SUMMARY}" "${LOGFILE}" <<'PYEOF'
import sys
import re
from datetime import datetime
from collections import defaultdict

starts_file, ends_file, summary_file, logfile = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# Parse start events with their timestamps
rule_starts = []  # (datetime, rule_name)
with open(starts_file) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 4:
            ts = datetime.strptime(f"{parts[1]} {parts[2]}", "%Y-%m-%d %H:%M:%S")
            rule = parts[3]
            rule_starts.append((ts, rule))

# Parse end events
rule_ends = []  # (datetime, rule_name, jobid)
with open(ends_file) as f:
    for line in f:
        parts = line.strip().split()
        if len(parts) >= 5:
            ts = datetime.strptime(f"{parts[1]} {parts[2]}", "%Y-%m-%d %H:%M:%S")
            rule = parts[3]
            jobid = parts[4]
            rule_ends.append((ts, rule, jobid))

# Also parse from the snakemake log more precisely: match jobid from start blocks
# Parse start blocks with jobid
jobid_starts = {}
with open(logfile) as f:
    content = f.read()

# Find all job start blocks (rule name + jobid + timestamp)
start_pattern = re.compile(
    r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\].*?(?:localrule|rule) (\w+):.*?jobid: (\d+)',
    re.DOTALL
)

for match in start_pattern.finditer(content):
    ts_str, rule, jobid = match.groups()
    ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
    jobid_starts[jobid] = (ts, rule)

# Build timing records
records = []
for end_ts, rule, jobid in rule_ends:
    if jobid in jobid_starts:
        start_ts, start_rule = jobid_starts[jobid]
        duration = (end_ts - start_ts).total_seconds()
        records.append((rule, jobid, start_ts, end_ts, duration))

# Write summary
with open(summary_file, 'w') as f:
    f.write("rule\tjob_id\tstart\tend\tduration_sec\n")
    for rule, jobid, start, end, dur in sorted(records, key=lambda x: x[2]):
        f.write(f"{rule}\t{jobid}\t{start}\t{end}\t{dur:.0f}\n")

# Print per-rule aggregate
print(f"\n{'Rule':<30} {'Count':>6} {'Total(s)':>10} {'Mean(s)':>10} {'Min(s)':>10} {'Max(s)':>10}")
print("-" * 80)

rule_durations = defaultdict(list)
for rule, _, _, _, dur in records:
    rule_durations[rule].append(dur)

for rule in sorted(rule_durations.keys()):
    durs = rule_durations[rule]
    print(f"{rule:<30} {len(durs):>6} {sum(durs):>10.0f} {sum(durs)/len(durs):>10.1f} {min(durs):>10.0f} {max(durs):>10.0f}")

total = sum(d for durs in rule_durations.values() for d in durs)
print("-" * 80)
print(f"{'TOTAL (sum of all jobs)':<30} {'':>6} {total:>10.0f}")

PYEOF

# Also report snakemake benchmark files if they exist
if ls "${TIMING_DIR}"/busco/*.txt 1>/dev/null 2>&1; then
    echo ""
    echo "=== Snakemake benchmark files (BUSCO per-sample) ==="
    echo -e "sample\ts\th:m:s\tmax_rss\tmax_vms\tmax_uss\tmax_pss\tio_in\tio_out\tmean_load\tcpu_time"
    for f in "${TIMING_DIR}"/busco/*.txt; do
        sample=$(basename "$f" .txt | sed 's/\.eukaryota_odb12//')
        tail -1 "$f" | awk -v s="$sample" '{printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", s, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10}'
    done
fi

# Cleanup temp files
rm -f "${TIMING_DIR}/_starts.tmp" "${TIMING_DIR}/_ends.tmp"

echo ""
echo "Full log:      ${LOGFILE}"
echo "Timing summary: ${TIMING_SUMMARY}"
echo "Benchmark dir:  ${TIMING_DIR}/"
