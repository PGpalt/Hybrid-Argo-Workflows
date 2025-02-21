#!/bin/bash
#SBATCH --job-name=mnist_distributed
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00

# Launch distributed training via srun.
# SLURM will set environment variables (like RANK, WORLD_SIZE) for distributed jobs.
srun python3 distributed-mnist.py --lr=${LR} --batch-size=32 --epochs=${epochs} --log-path=mnist.log