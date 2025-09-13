import argparse
import io
import subprocess
import os
import time
from clearml import Task, TaskTypes, Dataset
from clearml.backend_api.session.client import APIClient
from clearml.backend_api.session import Session


def get_running_slurm_jobs():
    username = subprocess.check_output("whoami", shell=True).decode().strip()
    result = subprocess.run(f"squeue --noheader --user {username} | wc -l", shell=True, capture_output=True, text=True)
    return int(result.stdout.strip())


def resolve_container(task):
    source_type = task.get_parameter("slurm/container_source/type", default="none")

    match source_type:
        case "docker_url":
            return {"type": "docker", "docker_url": task.get_parameter("slurm/container_source/docker_url")}
        case "sif_path":
            return {"type": "sif", "sif_path": task.get_parameter("slurm/container_source/sif_path")}
        case "artifact_task":
            project = task.get_parameter("slurm/container_source/project")
            dataset_name = task.get_parameter("slurm/container_source/dataset_name")
            dataset = Dataset.get(dataset_project=project, dataset_name=dataset_name)
            return {"type": "artifact", "dataset_id": dataset.id}
        case _:
            raise ValueError(f"Invalid container_source/type: {source_type}")


def build_singularity_command(task, task_id, extra_envs):
    use_singularity = task.get_parameter("slurm/use_singularity", default=False)

    if not use_singularity:
        return f"clearml-agent execute --id {task_id}"

    container = resolve_container(task)
    gpus = int(task.get_parameter("slurm/gpu", 0))
    use_nv = "--nv" if gpus > 0 else ""

    bind_paths = ["${SLURM_TMPDIR}:/tmp"]
    overlay = task.get_parameter("slurm/singularity_overlay", default="")
    overlay_arg = f"--overlay {overlay}:rw" if overlay else ""

    if not overlay:
        bind_paths.append("${SLURM_TMPDIR}:${HOME}")

    binds = task.get_parameter("slurm/singularity_binds", default="")
    if binds:
        bind_paths.extend(b.strip() for b in binds.split(","))

    bind_arg = f"--bind {','.join(bind_paths)}"
    env_args = (
        "--env CLEARML_TASK_ID=$CLEARML_TASK_ID "
        "--env CLEARML_API_HOST=$CLEARML_API_HOST "
        "--env CLEARML_WEB_HOST=$CLEARML_WEB_HOST "
        "--env CLEARML_FILES_HOST=$CLEARML_FILES_HOST "
        "--env CLEARML_API_ACCESS_KEY=$CLEARML_API_ACCESS_KEY "
        "--env CLEARML_API_SECRET_KEY=$CLEARML_API_SECRET_KEY"
    )

    # Add extra environment variables
    for env_key in extra_envs:
        env_args += f" --env {env_key}=${env_key}"

    # Add CUDA_VERSION=12.9 if only CPUs are used
    if gpus == 0:
        env_args += " --env CUDA_VERSION=12.9"

    clearml_cmd = f"clearml-agent execute --id {task_id}"

    match container["type"]:
        case "docker":
            return (
                f"singularity exec {use_nv} --containall --cleanenv {overlay_arg} {bind_arg} {env_args} "
                f"{container['docker_url']} {clearml_cmd}"
            )
        case "sif":
            return (
                f"singularity exec {use_nv} --containall --cleanenv {overlay_arg} {bind_arg} {env_args} "
                f"{container['sif_path']} {clearml_cmd}"
            )
        case "artifact":
            dataset_cache_path = f"$SLURM_TMPDIR/.clearml/cache/storage_manager/datasets/ds_{container['dataset_id']}"

            # Add extra environment variables to fetch command as well
            extra_env_args = ""
            for env_key in extra_envs:
                extra_env_args += f" --env {env_key}=${env_key}"

            fetch_cmd = (
                "singularity exec --containall --cleanenv "
                "--bind $SLURM_TMPDIR:/tmp "
                "--bind $SLURM_TMPDIR:$HOME "
                "--env CLEARML_API_HOST=$CLEARML_API_HOST "
                "--env CLEARML_WEB_HOST=$CLEARML_WEB_HOST "
                "--env CLEARML_FILES_HOST=$CLEARML_FILES_HOST "
                "--env CLEARML_API_ACCESS_KEY=$CLEARML_API_ACCESS_KEY "
                "--env CLEARML_API_SECRET_KEY=$CLEARML_API_SECRET_KEY "
                f"{extra_env_args} "
                "docker://thewillyp/clearml-agent "
                f"clearml-data get --id {container['dataset_id']}"
            )

            run_cmd = (
                f"singularity exec {use_nv} --containall --cleanenv {overlay_arg} {bind_arg} {env_args} "
                f"$(find {dataset_cache_path} -name '*.sif' | head -1) {clearml_cmd}"
            )
            return f"{fetch_cmd} && {run_cmd}"
        case _:
            raise ValueError(f"Unknown container type: {container['type']}")


def create_sbatch_script(task, task_id, command, log_dir, extra_envs):
    session = Session()
    access_key = session.access_key
    secret_key = session.secret_key

    # Get files host and web host from session config
    api_host = session.config.get("api.api_server", None)
    files_host = session.config.get("api.files_server", None)
    web_host = session.config.get("api.web_server", None)

    gpus = int(task.get_parameter("slurm/gpu", 0))
    gpu_directive = f"#SBATCH --gres=gpu:{gpus}" if gpus > 0 else ""
    use_singularity = task.get_parameter("slurm/use_singularity", default=True)

    # Get setup commands from task parameters
    setup_commands = task.get_parameter("slurm/setup_commands", default="")
    setup_section = ""
    if setup_commands:
        setup_section = f"\n# Setup commands\n{setup_commands}\n"

    # Export extra environment variables
    extra_env_exports = ""
    if extra_envs:
        extra_env_exports = "\n# Extra environment variables\n"
        for env_key in extra_envs:
            env_value = os.environ.get(env_key, "")
            extra_env_exports += f'export {env_key}="{env_value}"\n'

    ssh_setup = ""
    if use_singularity:
        ssh_setup = """# Copy SSH directory to SLURM_TMPDIR
mkdir -p ${{SLURM_TMPDIR}}/.ssh
cp -r ${{HOME}}/.ssh/* ${{SLURM_TMPDIR}}/.ssh/
chmod 700 ${{SLURM_TMPDIR}}/.ssh
chmod 600 ${{SLURM_TMPDIR}}/.ssh/*"""

    return f"""#!/bin/bash
#SBATCH --job-name=clearml_task
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --mem={task.get_parameter("slurm/memory")}
#SBATCH --time={task.get_parameter("slurm/time")}
#SBATCH --cpus-per-task={task.get_parameter("slurm/cpu")}
#SBATCH --output={log_dir}/run-%j-{task_id}.log
#SBATCH --error={log_dir}/run-%j-{task_id}.err
{gpu_directive}

export CLEARML_TASK_ID={task_id}
export CLEARML_API_HOST="{api_host}"
export CLEARML_WEB_HOST="{web_host}"
export CLEARML_FILES_HOST="{files_host}"
export CLEARML_API_ACCESS_KEY="{access_key}"
export CLEARML_API_SECRET_KEY="{secret_key}"
{extra_env_exports}
{ssh_setup}
{setup_section}
{command}
"""


def submit_slurm_job(sbatch_script):
    try:
        result = subprocess.run(["sbatch"], input=sbatch_script, text=True, capture_output=True, check=True)
        print(f"[INFO] sbatch stdout: {result.stdout}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] sbatch stderr: {e.stderr}")
        return e.stdout or ""


def run(queue_name, envs, max_jobs, lazy_poll_interval):
    client = APIClient()

    # Get queue ID by name
    queues_response = client.queues.get_all(name=queue_name)
    if not queues_response:
        raise ValueError(f"Queue '{queue_name}' not found")
    queue_id = queues_response[0].id
    print(f"[INFO] Found queue '{queue_name}' with ID: {queue_id}")

    while True:
        try:
            current_jobs = get_running_slurm_jobs()

            if current_jobs >= max_jobs:
                print(f"[INFO] Max jobs ({max_jobs}) reached, sleeping...")
                time.sleep(lazy_poll_interval)
                continue

            # Check how many tasks are in the queue
            num_entries_response = client.queues.get_num_entries(queue=queue_id)
            num_entries = num_entries_response.num

            if num_entries == 0:
                print(f"[INFO] No tasks in queue, lazy polling...")
                time.sleep(lazy_poll_interval)
                continue

            print(f"[INFO] Found {num_entries} tasks in queue, fast polling...")

            # Fast polling loop - dequeue all available tasks
            tasks_processed = 0
            while tasks_processed < num_entries:
                # Check job limit again
                current_jobs = get_running_slurm_jobs()
                if current_jobs >= max_jobs:
                    print(f"[INFO] Hit max jobs limit during burst, processed {tasks_processed}/{num_entries}")
                    break

                print(f"[INFO] Attempting to dequeue task from '{queue_name}'...")
                response = client.queues.get_next_task(queue=queue_id)

                if not response.entry:
                    print(f"[INFO] No more tasks available, processed {tasks_processed}/{num_entries}")
                    break

                task_id = response.entry.task
                task = Task.get_task(task_id=task_id)

                log_dir = task.get_parameter("slurm/log_dir")

                command = build_singularity_command(task, task_id, envs)
                sbatch_script = create_sbatch_script(task, task_id, command, log_dir, envs)

                print(f"[INFO] Submitting SLURM job for task {task_id}")
                submit_slurm_job(sbatch_script)

                tasks_processed += 1

        except Exception as e:
            print(f"[ERROR] {e}")
        time.sleep(lazy_poll_interval)


def main():
    parser = argparse.ArgumentParser(description="Convert ClearML jobs to SLURM jobs")
    parser.add_argument("--queue", required=True, help="clearml queue name to pull jobs from")
    parser.add_argument("--envs", required=True, help="comma separated list of env vars to pass to the job")
    parser.add_argument("--max_jobs", required=True, help="maximum number of concurrent SLURM jobs")
    parser.add_argument("--poll_interval", required=True, help="seconds between polling clearml server for new jobs")

    args = parser.parse_args()

    queue_name, envs = args.queue, args.envs
    envs = envs.split(",") if envs else []
    max_jobs = int(args.max_jobs)
    poll_interval = float(args.poll_interval)

    run(queue_name, envs, max_jobs, poll_interval)


if __name__ == "__main__":
    main()
