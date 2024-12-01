#!/bin/bash
#SBATCH --job-name=my_script    # Job name
#SBATCH --ntasks=2              # Number of tasks
#SBATCH --time=00:01:00         # Run time (hh:mm:ss) - 1 minute 
#SBATCH --account=<TRAINING_PROJECT>

echo "Start at `date`"

# Job steps
srun hostname
srun sleep 30

echo "End at `date`"