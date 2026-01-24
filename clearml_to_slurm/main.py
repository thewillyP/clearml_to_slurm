import argparse
import subprocess
import os
import time
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from clearml import Task
from clearml.backend_api.session.client import APIClient
from clearml.backend_api.session import Session


TEMPLATE_DIR = Path(__file__).parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


def ssh_cmd(cmd: list[str], input_text: str | None = None) -> subprocess.CompletedProcess:
    """Run command via SSH to localhost."""
    return subprocess.run(
        ["ssh", "localhost"] + cmd,
        input=input_text,
        capture_output=True,
        text=True,
    )


def get_running_job_count() -> int:
    result = ssh_cmd(["squeue", "--noheader", "--user", os.environ["USER"]])
    return len(result.stdout.strip().split("\n")) if result.stdout.strip() else 0


def submit_job(script: str) -> str:
    result = ssh_cmd(["sbatch"], input_text=script)
    if result.returncode != 0:
        print(f"[ERROR] sbatch failed: {result.stderr}")
    else:
        print(f"[INFO] {result.stdout.strip()}")
    return result.stdout


def build_sbatch_script(
    task: Task,
    task_id: str,
    config_file: str,
    account: str,
    extra_env_keys: list[str],
) -> str:
    session = Session()
    gpus = int(task.get_parameter("slurm/gpu", 0))
    skip_python_env = task.get_parameter("slurm/skip_python_env_install", default=False, cast=True)

    extra_envs = {k: os.environ.get(k, "") for k in extra_env_keys}
    if gpus == 0:
        extra_envs["CUDA_VERSION"] = "12.9"
    if skip_python_env:
        extra_envs["CLEARML_AGENT_SKIP_PYTHON_ENV_INSTALL"] = "1"

    template = jinja_env.get_template("job.sh.j2")
    return template.render(
        task_id=task_id,
        memory=task.get_parameter("slurm/memory"),
        time=task.get_parameter("slurm/time"),
        cpu=task.get_parameter("slurm/cpu"),
        gpu=gpus,
        account=account,
        log_dir=task.get_parameter("slurm/log_dir"),
        api_host=session.config.get("api.api_server"),
        web_host=session.config.get("api.web_server"),
        files_host=session.config.get("api.files_server"),
        access_key=session.access_key,
        secret_key=session.secret_key,
        extra_envs=extra_envs,
        config_file=config_file,
    )


def run(
    queue_name: str,
    extra_env_keys: list[str],
    max_jobs: int,
    poll_interval: float,
    config_file: str,
    account: str,
):
    client = APIClient()

    queues = client.queues.get_all(name=queue_name)
    if not queues:
        raise ValueError(f"Queue '{queue_name}' not found")
    queue_id = queues[0].id
    print(f"[INFO] Using queue '{queue_name}' (id={queue_id})")

    while True:
        try:
            if get_running_job_count() >= max_jobs:
                print(f"[INFO] At max jobs ({max_jobs}), waiting...")
                time.sleep(poll_interval)
                continue

            num_entries = client.queues.get_num_entries(queue=queue_id).num
            if num_entries == 0:
                print("[INFO] Queue empty, waiting...")
                time.sleep(poll_interval)
                continue

            print(f"[INFO] {num_entries} tasks queued, processing...")

            for _ in range(num_entries):
                if get_running_job_count() >= max_jobs:
                    print("[INFO] Hit max jobs during burst")
                    break

                response = client.queues.get_next_task(queue=queue_id)
                if not response.entry:
                    break

                task_id = response.entry.task
                task = Task.get_task(task_id=task_id)

                script = build_sbatch_script(task, task_id, config_file, account, extra_env_keys)

                print(f"[INFO] Submitting task {task_id}")
                submit_job(script)

        except Exception as e:
            print(f"[ERROR] {e}")

        time.sleep(poll_interval)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", required=True)
    parser.add_argument("--envs", default="")
    parser.add_argument("--max_jobs", type=int, required=True)
    parser.add_argument("--poll_interval", type=float, required=True)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--account", default="")
    args = parser.parse_args()

    env_keys = [e.strip() for e in args.envs.split(",") if e.strip()]
    run(args.queue, env_keys, args.max_jobs, args.poll_interval, args.config_file, args.account)


if __name__ == "__main__":
    main()
