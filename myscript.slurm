#!/bin/bash -l

#SBATCH --job-name=my_job    # Job name
#SBATCH --output=my_job.%j.out # Stdout (%j expands to jobId)
#SBATCH --error=my_job.%j.err # Stderr (%j expands to jobId)
#SBATCH --ntasks=1     # Number of tasks(processes)
#SBATCH --nodes=1     # Number of nodes requested
#SBATCH --ntasks-per-node=1     # Tasks per node
#SBATCH --cpus-per-task=1     # Threads per task
#SBATCH --time=00:01:00   # walltime
#SBATCH --mem=10M   # memory per NODE
#SBATCH --partition=compute    # Partition
#SBATCH --account=pa210701    # Replace with your system project

# load necessary modules
module load gnu/4.9.2
module load intel/15.0.3
module load intelmpi/5.0.3

## RUN YOUR PROGRAM ##
srun echo hello