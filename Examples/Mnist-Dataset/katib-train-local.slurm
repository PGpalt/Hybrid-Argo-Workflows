#!/bin/bash
#SBATCH --job-name=mnist_distributed
#SBATCH --nodes=1
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00
#SBATCH --out=/root/slurm.out

# Launch distributed training via srun.
# SLURM will set environment variables (like RANK, WORLD_SIZE) for distributed jobs.
srun python3 distributed-mnist-local.py --lr=${LR} --batch-size=32 --epochs=${epochs} --log-path=mnist.log