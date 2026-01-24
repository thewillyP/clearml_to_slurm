#!/bin/bash
# =============================================================================
# CLUSTER CONFIGURATION
# =============================================================================
# This file runs at the start of both the bridge job and each task job.
# Configure two things:
#   1. SETUP: Module loads, container builds, auth
#   2. WRAPPER: How to execute commands
#
# SSH REQUIREMENT: The bridge runs slurm commands via `ssh localhost`.
# Ensure passwordless SSH is configured.
# =============================================================================

# === SETUP ===

# Container paths
SIF_DIR="/scratch/wlp9800/singularity"
SIF_CPU="${SIF_DIR}/devenv-cpu.sif"
SIF_GPU="${SIF_DIR}/devenv-gpu.sif"

# Create directory if needed
mkdir -p "$SIF_DIR"

# Build containers if they don't exist
if [[ ! -f "$SIF_CPU" ]]; then
    singularity build "$SIF_CPU" docker://thewillyp/devenv:cpu
fi

if [[ ! -f "$SIF_GPU" ]]; then
    singularity build "$SIF_GPU" docker://thewillyp/devenv:gpu
fi

# Prepare tmpdir structure (for --containall)
mkdir -p "${SLURM_TMPDIR}/tmp"
mkdir -p "${SLURM_TMPDIR}/home"

# Copy SSH keys into tmpdir home so they're available inside container
if [[ -d "$HOME/.ssh" ]]; then
    cp -r "$HOME/.ssh" "${SLURM_TMPDIR}/home/.ssh"
    chmod 700 "${SLURM_TMPDIR}/home/.ssh"
    chmod 600 "${SLURM_TMPDIR}/home/.ssh"/* 2>/dev/null || true
fi

# Auth via AWS Parameter Store (using singularity to run aws cli)
aws_ssm_get() {
    singularity run --cleanenv \
        --env AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
        --env AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
        --env AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
        docker://amazon/aws-cli \
        ssm get-parameter --name "$1" --with-decryption --query Parameter.Value --output text
}

export CLEARML_API_ACCESS_KEY=$(aws_ssm_get /dev/research/clearml_api_access_key)
export CLEARML_API_SECRET_KEY=$(aws_ssm_get /dev/research/clearml_api_secret_key)
export CLEARML_API_HOST=$(aws_ssm_get /dev/research/clearml_api_host)
export CLEARML_WEB_HOST=$(aws_ssm_get /dev/research/clearml_web_host)
export CLEARML_FILES_HOST=$(aws_ssm_get /dev/research/clearml_files_host)


# === WRAPPER ===

# Singularity (default)
# Uses USE_GPU env var to select container. Bridge sets USE_GPU=0, tasks set based on slurm/gpu param.
wrapper() {
    if [[ "${USE_GPU:-0}" == "1" ]]; then
        SIF_PATH="$SIF_GPU"
        NV_FLAG="--nv"
    else
        SIF_PATH="$SIF_CPU"
        NV_FLAG=""
    fi

    singularity exec $NV_FLAG \
        --containall \
        --cleanenv \
        --bind "${SLURM_TMPDIR}/tmp:/tmp" \
        --bind "${SLURM_TMPDIR}/home:$HOME" \
        --env CLEARML_API_ACCESS_KEY="$CLEARML_API_ACCESS_KEY" \
        --env CLEARML_API_SECRET_KEY="$CLEARML_API_SECRET_KEY" \
        --env CLEARML_API_HOST="$CLEARML_API_HOST" \
        --env CLEARML_WEB_HOST="$CLEARML_WEB_HOST" \
        --env CLEARML_FILES_HOST="$CLEARML_FILES_HOST" \
        --env HOME="$HOME" \
        --env USER="$USER" \
        "$SIF_PATH" "$@"
}

# No container:
# wrapper() { "$@"; }