#!/bin/bash -l

#SBATCH --job-name=Genomic-QC-Alignment-%j
#SBATCH --output=Genomic-QC-Alignment.%j.out
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1
#SBATCH --mem=16G   # memory per NODE
##SBATCH --mem-per-cpu=3968M   # memory per CPU core
#SBATCH --time=00:20:00
#SBATCH --partition=compute
#SBATCH --account=pa251203

set -euo pipefail

# --- Conda (use WORKDIR for cache + env) ------------------------------------
: "${WORKDIR:?WORKDIR is not set}"
module purge
module load anaconda/2024.10
source "$(conda info --base)/etc/profile.d/conda.sh"

export CONDA_PKGS_DIRS="$WORKDIR/conda/pkgs"
conda activate "$WORKDIR/conda/envs/bioalign"

# --- CONFIG: all files in $PWD ----------------------------------------------
REF="./${DATAPATH}/genome.fa"
R1=./sample_R1.trimmed.fastq.gz
R2=./sample_R2.trimmed.fastq.gz
OUT_DIR="${DATAPATH}"

mkdir -p "$OUT_DIR"

base=$(basename "$REF" .fa)

# 1 Build Bowtie2 index
bowtie2-build "$REF" "${OUT_DIR}/${base}_idx"

# 2 Align paired-end reads
bowtie2 \
  -x "${OUT_DIR}/${base}_idx" \
  -1 "$R1" \
  -2 "$R2" \
  -p "$SLURM_CPUS_PER_TASK" \
  -S "${OUT_DIR}/${base}.sam"

# 3 Convert SAM→BAM, sort & index
samtools view -@ 4 -bS "${OUT_DIR}/${base}.sam" \
  | samtools sort -@ 4 -o "${OUT_DIR}/${base}.sorted.bam"
samtools index "${OUT_DIR}/${base}.sorted.bam"
samtools flagstat "${OUT_DIR}/${base}.sorted.bam" > "${OUT_DIR}/alignment_summary.txt"