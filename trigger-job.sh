#!/bin/bash
# trigger-job.sh

# Extract the suffix from POD_NAME (populated from the fieldRef in the pod spec)
SUFFIX=${POD_NAME##*-}
echo "Pod suffix: ${SUFFIX}"
# save the path that will be created for this job and will serve as an output to the workflow
echo ${SUFFIX} > /slurm-job-out-path.txt

# Create a directory for the SLURM job on the remote host using the suffix
sshpass -p 'root' ssh -o StrictHostKeyChecking=no -p 2222 root@host.minikube.internal "mkdir -p slurm-job-${SUFFIX}"

# for file in /tmp/*; do       
#     # Transfer the files to the remote SLURM machine via SCP
#     sshpass -p 'root' scp -o StrictHostKeyChecking=no -P 2222 "${file}" "root@host.minikube.internal:slurm-job-${SUFFIX}/${file##*/}"
# done

if [ "${TRANSFER_DATA}" != "NotSet" ]; then
    # Transfer the files to the remote SLURM machine via SCP
    sshpass -p 'root' scp -r -o StrictHostKeyChecking=no -P 2222 /tmp/* "root@host.minikube.internal:slurm-job-${SUFFIX}/"
fi

# Run the SLURM command on the remote machine and capture the output
output=$(sshpass -p 'root' ssh -o StrictHostKeyChecking=no -p 2222 root@host.minikube.internal "cd slurm-job-${SUFFIX} && ${COMMAND}")
echo "Submission output: ${output}"

# Extract the job ID (assumes the job id is the 4th word)
job_id=$(echo ${output} | awk '{print $4}')
echo "Job submitted with ID: ${job_id}"

if [ "$(echo ${output} | awk '{print $1}')" = "Submitted" ]; then
  # Poll the job status until it is no longer in squeue
  while sshpass -p 'root' ssh -o StrictHostKeyChecking=no -p 2222 root@host.minikube.internal "squeue -j ${job_id}" | grep -q "${job_id}"; do
      echo "Job ${job_id} is still running. Waiting..."
      sleep 5
  done        
  echo "Job ${job_id} has completed."
fi

# Transfer the SLURM output back to the pod
sshpass -p 'root' scp -o StrictHostKeyChecking=no -P 2222 root@host.minikube.internal:slurm-job-${SUFFIX}/mnist.log /katib/mnist.log
