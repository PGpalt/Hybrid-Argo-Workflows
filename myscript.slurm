#!/bin/bash -l

#SBATCH --job-name=mnist_distributed    # Job name
#SBATCH --output=mnist_distributed.%j.out # Stdout (%j expands to jobId)
#SBATCH --error=mnist_distributed.%j.err # Stderr (%j expands to jobId)
#SBATCH --ntasks=2     # Number of tasks(processes)
#SBATCH --nodes=1     # Number of nodes requested
#SBATCH --cpus-per-task=1     # Threads per task
#SBATCH --time=00:05:00   # walltime
#SBATCH --partition=compute    # Partition
#SBATCH --account=pr016051_thin    # Replace with your system project

if [ x$SLURM_CPUS_PER_TASK == x ]; then
  export OMP_NUM_THREADS=1
else
  export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
fi


## LOAD MODULES ##
module purge        # clean up loaded modules 

# load necessary modules
module load gnu/8
module load cuda/10.1.168
module load python/3.7.6
module load pytorch/1.9.1

## RUN YOUR PROGRAM ##
srun python3 distributed-mnist.py --lr=0.0017 --batch-size=32 --epochs=1 --log-path=mnist.log