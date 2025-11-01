#!/bin/bash -l

#SBATCH --job-name=Genomic-QC-Alignment
#SBATCH --output=Genomic-QC-Alignment.out
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=4
#SBATCH --nodes=1     # Number of nodes requested
#SBATCH --time=00:20:00

# Load ARIS modules
module purge
module load gnu/4.9.2
module load bowtie/2.2.6
module load samtools/1.9

# --- CONFIG: all files in $PWD ----------------------------------------------

REF=./genome.fa
R1=./sample_R1.trimmed.fastq.gz
R2=./sample_R2.trimmed.fastq.gz
OUT_DIR=.

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
