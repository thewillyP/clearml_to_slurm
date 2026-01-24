#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/cluster_config.sh"

LOG_DIR="${1:?Usage: entrypoint.sh LOG_DIR QUEUE_NAME [ACCOUNT] [RUN_TIME] [RUN_CPUS] [RUN_MEM] [MAX_JOBS] [POLL_INTERVAL] [ENVS]}"
QUEUE_NAME="${2:?}"
ACCOUNT="${3:-}"
RUN_TIME="${4:-0-06:00:00}"
RUN_CPUS="${5:-2}"
RUN_MEM="${6:-4GB}"
MAX_JOBS="${7:-1950}"
POLL_INTERVAL="${8:-30}"
ENVS="${9:-}"

ACCOUNT_DIRECTIVE=""
if [[ -n "$ACCOUNT" ]]; then
    ACCOUNT_DIRECTIVE="#SBATCH --account=${ACCOUNT}"
fi

sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=clearml_bridge
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=${RUN_MEM}
#SBATCH --time=${RUN_TIME}
#SBATCH --cpus-per-task=${RUN_CPUS}
${ACCOUNT_DIRECTIVE}
#SBATCH --output=${LOG_DIR}/clearml-bridge-%j.log
#SBATCH --error=${LOG_DIR}/clearml-bridge-%j.err

set -euo pipefail

export USE_GPU=0

source "${CONFIG_FILE}"

wrapper bash -c "pip install --quiet git+https://github.com/thewillyP/clearml_to_slurm.git && export PATH=\$HOME/.local/bin:\$PATH && export PYTHONUNBUFFERED=1 && to_slurm --queue '${QUEUE_NAME}' --envs '${ENVS}' --max_jobs ${MAX_JOBS} --poll_interval ${POLL_INTERVAL} --config-file '${CONFIG_FILE}' --account '${ACCOUNT}'"
EOF