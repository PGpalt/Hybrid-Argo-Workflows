#!/bin/bash
# trigger-job.sh
set -e

# point to the mounted key
SSH_KEY=/root/.ssh/id_rsa
SSH_OPTS="-i ${SSH_KEY} \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/nul"

# only add Port if SSH_PORT isn’t empty
if [ -n "${SSH_PORT}" ]; then
  SSH_OPTS="${SSH_OPTS} -o Port=${SSH_PORT}"
fi

if [ "${SLURM_INPUT}" == "false" ]; then
  # Extract the suffix from POD_NAME (populated from the fieldRef in the pod spec)
  SUFFIX=${POD_NAME##*-}
  echo "Pod suffix: ${SUFFIX}"

  if [ "${FILE_PATH}" == "NotSet" ]; then
    FILE_PATH="slurm-job-${SUFFIX}"
  fi

  echo "FILE_PATH: ${FILE_PATH}"

  # Create a directory for the SLURM job on the remote host using the suffix
  ssh ${SSH_OPTS} ${SSH_USER}@${SSH_HOST} "mkdir -p ${FILE_PATH}"

  if [ "${TRANSFER_DATA}" != "NotSet" ]; then
  # Transfer the files to the remote SLURM machine via SCP
    scp ${SSH_OPTS} -r /tmp/* ${SSH_USER}@${SSH_HOST}:${FILE_PATH}/
  fi
else
  SUFFIX=$(cat /tmp/slurm-job-out-path.txt)
  if [ "${FILE_PATH}" == "NotSet" ]; then
    FILE_PATH="slurm-job-${SUFFIX}"
  fi
fi

mkdir -p ${FILE_PATH_OUT}
# save the path that will be created for this job and will serve as an output to the workflow
echo ${SUFFIX} > /${FILE_PATH_OUT}/slurm-job-out-path.txt

# Run the SLURM command on the remote machine and capture the output
output=$(ssh ${SSH_OPTS} ${SSH_USER}@${SSH_HOST} "cd ${FILE_PATH} && ${COMMAND}")
echo "Submission output: ${output}"

# Extract the job ID (assumes the job id is the 4th word)
job_id=$(echo ${output} | awk '{print $4}')
echo "Job submitted with ID: ${job_id}"

if [ "$(echo ${output} | awk '{print $1}')" = "Submitted" ]; then
  # Poll the job status until it is no longer in squeue
  while ssh ${SSH_OPTS} ${SSH_USER}@${SSH_HOST} "squeue -j ${job_id}" | grep -q "${job_id}"; do
      echo "Job ${job_id} is still running. Waiting..."
      sleep 5
  done
  echo "Job ${job_id} has completed."
fi

# Transfer the SLURM output back to the pod and store the s3 artifact if required
if [ "${FILE_NAME}" != "NotSet" ] && [ "${FETCH_DATA}" == "true" ]; then
  if [[ "${FILE_NAME}" == /* ]]; then
    # FILE_NAME starts with '/', so use it directly on the remote host
    # and extract only the basename for the local destination.
    base_file=$(basename "${FILE_NAME}")
    scp ${SSH_OPTS} ${SSH_USER}@${SSH_HOST}:"${FILE_NAME}" "${FILE_PATH_OUT}/${base_file}"
  else
    # Otherwise, use the default remote path (inside the slurm-job-${SUFFIX} directory)
    scp ${SSH_OPTS} ${SSH_USER}@${SSH_HOST}:"${FILE_PATH}/${FILE_NAME}" \
      "${FILE_PATH_OUT}/${FILE_NAME}"
  fi
fi
