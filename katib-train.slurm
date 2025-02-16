#!/bin/bash
#SBATCH --job-name=mnist_distributed
#SBATCH --ntasks=2
#SBATCH --cpus-per-task=1
#SBATCH --time=00:20:00
#SBATCH --output=mnist_%j.out

# # Print the hyperparameters (set by the trial container)
# echo "Using learning rate: ${LR}"
# echo "Using batch size: ${BATCH_SIZE}"
# echo "Using epochs: ${EPOCHS}"

# Launch distributed training via srun.
# SLURM will set environment variables (like RANK, WORLD_SIZE) for distributed jobs.
srun python3 mnist_train.py --lr ${LR} --batch_size 32 --epochs ${epochs}