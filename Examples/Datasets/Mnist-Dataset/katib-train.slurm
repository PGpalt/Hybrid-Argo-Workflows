#!/bin/bash -l

#SBATCH --job-name=mnist_distributed    # Job name
#SBATCH --output=mnist_distributed.%j.out # Stdout (%j expands to jobId)
#SBATCH --error=mnist_distributed.%j.err # Stderr (%j expands to jobId)
#SBATCH --ntasks=2     # Number of tasks(processes)
#SBATCH --nodes=1     # Number of nodes requested
#SBATCH --cpus-per-task=1     # Threads per task
#SBATCH --time=00:15:00   # walltime
#SBATCH --mem=8G   # memory per NODE
##SBATCH --mem-per-cpu=3968M   # memory per CPU core
#SBATCH --partition=compute    # Partition
#SBATCH --account=pa251203    # Replace with your system project

if [ x$SLURM_CPUS_PER_TASK == x ]; then
  export OMP_NUM_THREADS=1
else
  export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
fi


## LOAD MODULES ##
module purge        # clean up loaded modules 

# load necessary modules
module load gnu/14
module load python/3.8
module load pytorch/2.9.0

## RUN YOUR PROGRAM ##
srun python3 distributed-mnist.py --lr=${LR} --batch-size=32 --epochs=${epochs} --log-path=mnist.log