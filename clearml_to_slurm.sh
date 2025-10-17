#!/bin/bash
RUN_TIME=${1:-"0-06:00:00"}  # default to 0 days and 6 hour
RUN_CPUS=${2:-"2"}
RUN_MEM=${3:-"4GB"}
LOG_DIR=$4  # replace with directory where you want to save logs
ENVS=${5:-""}  # comma-separated list of env vars to be made available in slurm job
QUEUE_NAME=$6  # specify your clearml queue
MAX_JOBS=${7:-1950}  # max number of jobs in parallel before throttling
POLL_INTERVAL=${8:-30}  # seconds between polling clearml server for new jobs

# Submit the SLURM job
sbatch <<EOF
#!/bin/bash
#SBATCH --job-name=clearml_agent
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem=${RUN_MEM}
#SBATCH --time=${RUN_TIME}
#SBATCH --cpus-per-task=${RUN_CPUS}
#SBATCH --output=${LOG_DIR}/clearml-agent-%j.log
#SBATCH --error=${LOG_DIR}/clearml-agent-%j.err

# add any module loads here if needed to set up your environment that contains python, pip, clearml
# delete the next line if not applicable
module load python/intel/3.8.6

pip install --upgrade git+https://github.com/thewillyP/clearml_to_slurm.git

to_slurm --queue ${QUEUE_NAME} --envs "${ENVS}" --max_jobs ${MAX_JOBS} --poll_interval ${POLL_INTERVAL}

EOF


