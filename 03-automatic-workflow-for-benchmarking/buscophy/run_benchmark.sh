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
BENCHMARK_DIR="results/benchmark"
TIMING_SUMMARY="${BENCHMARK_DIR}/timing.tsv"

mkdir -p "${BENCHMARK_DIR}"

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

# ── Aggregate Snakemake benchmark files ──────────────────────────────────────
echo ""
echo "=== Aggregating Snakemake benchmark files ==="

python3 - "${BENCHMARK_DIR}" "${TIMING_SUMMARY}" "${PIPELINE_ELAPSED}" <<'PYEOF'
import sys, os, csv
from pathlib import Path
from collections import defaultdict

benchmark_dir = Path(sys.argv[1])
summary_file  = sys.argv[2]
pipeline_wall = float(sys.argv[3])

# Snakemake benchmark columns:
# s  h:m:s  max_rss  max_vms  max_uss  max_pss  io_in  io_out  mean_load  cpu_time
COLS = ["s", "h_m_s", "max_rss", "max_vms", "max_uss", "max_pss",
        "io_in", "io_out", "mean_load", "cpu_time"]

# Collect all benchmark files grouped by rule (subdirectory name)
rule_data = defaultdict(list)   # rule -> list of dicts
for sub in sorted(benchmark_dir.iterdir()):
    if not sub.is_dir():
        continue
    rule_name = sub.name
    for txt in sorted(sub.glob("*.txt")):
        try:
            with open(txt) as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    entry = {"file": txt.name}
                    for col in COLS:
                        key = col if col != "h_m_s" else "h:m:s"
                        val = row.get(key, "")
                        if col == "h_m_s":
                            entry[col] = val
                        else:
                            try:
                                entry[col] = float(val)
                            except (ValueError, TypeError):
                                entry[col] = 0.0
                    rule_data[rule_name].append(entry)
        except Exception as e:
            print(f"  WARNING: could not parse {txt}: {e}", file=sys.stderr)

if not rule_data:
    print("No benchmark files found under", benchmark_dir)
    sys.exit(0)

# ── Write per-job detail TSV ─────────────────────────────────────────────────
with open(summary_file, "w") as f:
    f.write("rule\tsample\twall_s\th:m:s\tcpu_time_s\tmax_rss_MB\tmax_vms_MB\t"
            "max_uss_MB\tmax_pss_MB\tio_in_MB\tio_out_MB\tmean_load\n")
    for rule_name in sorted(rule_data):
        for e in sorted(rule_data[rule_name], key=lambda x: -x["s"]):
            sample = e["file"].replace(".txt", "")
            f.write(f"{rule_name}\t{sample}\t{e['s']:.2f}\t{e['h_m_s']}\t"
                    f"{e['cpu_time']:.2f}\t{e['max_rss']:.1f}\t{e['max_vms']:.1f}\t"
                    f"{e['max_uss']:.1f}\t{e['max_pss']:.1f}\t"
                    f"{e['io_in']:.1f}\t{e['io_out']:.1f}\t{e['mean_load']:.2f}\n")

# ── Per-rule aggregate table ─────────────────────────────────────────────────
header = (f"{'Rule':<25} {'Jobs':>6} {'Total(s)':>10} {'Mean(s)':>10} "
          f"{'Min(s)':>10} {'Max(s)':>10} {'CPU(s)':>10} "
          f"{'MaxRSS(MB)':>12} {'IO_out(MB)':>12}")
sep = "-" * len(header)

print()
print(header)
print(sep)

grand_wall  = 0.0
grand_cpu   = 0.0
grand_rss   = 0.0

# Define display order matching pipeline execution flow
RULE_ORDER = [
    "busco_download", "busco", "busco_summary",
    "get_nt", "get_aa", "filter_busco",
    "busco_mafft", "busco_trim", "busco_geneTrees",
    "paralogous_filtering", "clean_header",
    "busco_supermatrix", "busco_iqtree",
]

def sort_key(name):
    try:
        return RULE_ORDER.index(name)
    except ValueError:
        return len(RULE_ORDER)

for rule_name in sorted(rule_data, key=sort_key):
    entries   = rule_data[rule_name]
    walls     = [e["s"] for e in entries]
    cpus      = [e["cpu_time"] for e in entries]
    rss_vals  = [e["max_rss"] for e in entries]
    io_outs   = [e["io_out"] for e in entries]
    n         = len(entries)
    total_w   = sum(walls)
    total_cpu = sum(cpus)
    max_rss   = max(rss_vals)
    total_io  = sum(io_outs)

    print(f"{rule_name:<25} {n:>6} {total_w:>10.1f} {total_w/n:>10.1f} "
          f"{min(walls):>10.1f} {max(walls):>10.1f} {total_cpu:>10.1f} "
          f"{max_rss:>12.1f} {total_io:>12.1f}")

    grand_wall += total_w
    grand_cpu  += total_cpu
    grand_rss   = max(grand_rss, max_rss)

print(sep)
print(f"{'SUM (all jobs)':<25} {'':>6} {grand_wall:>10.1f} {'':>10} "
      f"{'':>10} {'':>10} {grand_cpu:>10.1f} "
      f"{grand_rss:>12.1f} {'':>12}")
print(f"{'PIPELINE WALL-CLOCK':<25} {'':>6} {pipeline_wall:>10.1f}")
print(f"{'PARALLEL SPEEDUP':<25} {'':>6} {grand_wall/pipeline_wall:>10.1f}x" if pipeline_wall > 0 else "")

# ── Per-rule resource summary TSV ────────────────────────────────────────────
resource_file = os.path.join(os.path.dirname(summary_file), "resource_summary.tsv")
with open(resource_file, "w") as f:
    f.write("rule\tjobs\ttotal_wall_s\tmean_wall_s\tmin_wall_s\tmax_wall_s\t"
            "total_cpu_s\tmax_rss_MB\tmean_rss_MB\ttotal_io_out_MB\n")
    for rule_name in sorted(rule_data, key=sort_key):
        entries   = rule_data[rule_name]
        walls     = [e["s"] for e in entries]
        cpus      = [e["cpu_time"] for e in entries]
        rss_vals  = [e["max_rss"] for e in entries]
        io_outs   = [e["io_out"] for e in entries]
        n         = len(entries)
        f.write(f"{rule_name}\t{n}\t{sum(walls):.2f}\t{sum(walls)/n:.2f}\t"
                f"{min(walls):.2f}\t{max(walls):.2f}\t{sum(cpus):.2f}\t"
                f"{max(rss_vals):.1f}\t{sum(rss_vals)/n:.1f}\t{sum(io_outs):.1f}\n")
    f.write(f"PIPELINE_TOTAL\t\t{pipeline_wall:.2f}\t\t\t\t{grand_cpu:.2f}\t"
            f"{grand_rss:.1f}\t\t\n")

print()
print(f"Per-job detail:     {summary_file}")
print(f"Resource summary:   {resource_file}")

PYEOF

echo ""
echo "Full log:           ${LOGFILE}"
echo "Benchmark dir:      ${BENCHMARK_DIR}/"
