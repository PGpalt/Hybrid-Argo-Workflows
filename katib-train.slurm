#!/bin/bash
#SBATCH --job-name=mnist_distributed
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00
#SBATCH --output=mnist_%j.out


# Launch distributed training via srun.
# SLURM will set environment variables (like RANK, WORLD_SIZE) for distributed jobs.
srun python3 mnist_train.py --lr ${LR} --batch_size 32 --epochs ${epochs}