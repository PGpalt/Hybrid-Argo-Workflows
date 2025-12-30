#!/bin/bash
# trigger-job.sh
set -e

SSH_KEY=/root/.ssh/id_rsa
SSH_OPTS="-i ${SSH_KEY} \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o GlobalKnownHostsFile=/dev/null \
  -o LogLevel=ERROR"


# only add Port if SSH_PORT isn’t empty
if [ -n "${SSH_PORT:-}" ]; then
  SSH_OPTS="${SSH_OPTS} -o Port=${SSH_PORT}"
fi

# Helper: run a remote command with login env (so WORKDIR may be defined)
rssh() {
  ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" "bash -lc '$*'"
}

# Try to discover remote WORKDIR; if it fails/empty -> fallback to original ($HOME-relative)
REMOTE_WORKDIR=""
USE_WORKDIR="false"

# Don't let a failing ssh here kill the whole script; we'll fallback.
set +e
REMOTE_WORKDIR=$(ssh ${SSH_OPTS} "${SSH_USER}@${SSH_HOST}" 'bash -lc "echo -n ${WORKDIR:-}"' 2>/dev/null)
rc=$?
set -e

if [ $rc -eq 0 ] && [ -n "${REMOTE_WORKDIR}" ]; then
  USE_WORKDIR="true"
  echo "Remote WORKDIR detected: ${REMOTE_WORKDIR}"
else
  echo "Remote WORKDIR not available (rc=${rc}). Falling back to HOME-based job directory."
fi

# --- SLURM_INPUT flow --------------------------------------------------------
if [ "${SLURM_INPUT}" = "false" ]; then
  SUFFIX=${POD_NAME##*-}
  echo "Pod suffix: ${SUFFIX}"

  if [ "${INPUT_FILE_PATH:-NotSet}" = "NotSet" ]; then
    INPUT_FILE_PATH="slurm-job-${SUFFIX}"
  fi

  # If INPUT_FILE_PATH is relative:
  # - prefer WORKDIR if detected
  # - otherwise keep it relative (=> remote home, original behavior)
  if [[ "${INPUT_FILE_PATH}" != /* ]]; then
    if [ "${USE_WORKDIR}" = "true" ]; then
      INPUT_FILE_PATH="${REMOTE_WORKDIR}/${INPUT_FILE_PATH}"
    fi
  fi

  # Create a directory for the SLURM job on the remote host
  rssh "mkdir -p \"${INPUT_FILE_PATH}\""

  if [ "${TRANSFER_DATA:-NotSet}" != "NotSet" ]; then
    # Transfer the files to the remote SLURM machine via SCP
    scp ${SSH_OPTS} -r /tmp/* "${SSH_USER}@${SSH_HOST}:${INPUT_FILE_PATH}/"
  fi
else
  SUFFIX=$(cat /tmp/slurm-job-out-path.txt)

  if [ "${INPUT_FILE_PATH:-NotSet}" = "NotSet" ]; then
    INPUT_FILE_PATH="slurm-job-${SUFFIX}"
  fi

  if [[ "${INPUT_FILE_PATH}" != /* ]]; then
    if [ "${USE_WORKDIR}" = "true" ]; then
      INPUT_FILE_PATH="${REMOTE_WORKDIR}/${INPUT_FILE_PATH}"
    fi
  fi
fi

# --- Local output dir (inside the pod) --------------------------------------
# Your Argo template typically passes OUTPUT_FILE_PATH like "slurm-out"
# and expects files under /slurm-out
if [ -z "${OUTPUT_FILE_PATH:-}" ]; then
  OUTPUT_FILE_PATH="slurm-out"
fi

if [[ "${OUTPUT_FILE_PATH}" == /* ]]; then
  LOCAL_OUT_DIR="${OUTPUT_FILE_PATH}"
else
  LOCAL_OUT_DIR="/${OUTPUT_FILE_PATH}"
fi

mkdir -p "${LOCAL_OUT_DIR}"
echo "${SUFFIX}" > "${LOCAL_OUT_DIR}/slurm-job-out-path.txt"

echo "INPUT_FILE_PATH (remote): ${INPUT_FILE_PATH}"
echo "LOCAL_OUT_DIR (pod): ${LOCAL_OUT_DIR}"
echo "USE_WORKDIR: ${USE_WORKDIR}"

# --- Run the SLURM command on the remote machine ----------------------------
output=$(rssh "cd \"${INPUT_FILE_PATH}\" && ${COMMAND}")
echo "Submission output: ${output}"

# Extract job ID (assumes: Submitted batch job <id>)
job_id=$(echo "${output}" | awk '{print $4}')
echo "Job submitted with ID: ${job_id}"

if [ "$(echo "${output}" | awk '{print $1}')" = "Submitted" ]; then
  while rssh "squeue -j ${job_id}" | grep -q "${job_id}"; do
    echo "Job ${job_id} is still running. Waiting..."
    sleep 5
  done
  echo "Job ${job_id} has completed."
fi

# --- Fetch output file back to the pod (if requested) -----------------------
if [ "${OUTPUT_FILE_NAME:-NotSet}" != "NotSet" ] && [ "${FETCH_DATA:-false}" = "true" ]; then
  if [[ "${OUTPUT_FILE_NAME}" == /* ]]; then
    base_file=$(basename "${OUTPUT_FILE_NAME}")
    scp ${SSH_OPTS} -r "${SSH_USER}@${SSH_HOST}:${OUTPUT_FILE_NAME}" "${LOCAL_OUT_DIR}/${base_file}"
  else
    base_file=$(basename "${OUTPUT_FILE_NAME}")
    scp ${SSH_OPTS} -r "${SSH_USER}@${SSH_HOST}:${INPUT_FILE_PATH}/${OUTPUT_FILE_NAME}" \
      "${LOCAL_OUT_DIR}/${base_file}"
  fi
fi

if [ "${CLEAN_DATA}" != "false" ] && [ "${OUTPUT_FILE_NAME:-NotSet}" != "NotSet" ]; then
  OUTPUT_FILE_NAME="${OUTPUT_FILE_NAME%/}"

  if [[ "${OUTPUT_FILE_NAME}" == /* ]]; then
    REMOTE_CLEAN="${OUTPUT_FILE_NAME}"
  else
    REMOTE_CLEAN="${INPUT_FILE_PATH}/${OUTPUT_FILE_NAME}"
  fi

  rssh "rm -rf -- \"${REMOTE_CLEAN}\""
fi
