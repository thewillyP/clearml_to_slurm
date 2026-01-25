import argparse
import json
import sys
import os
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from clearml import Task
from clearml.backend_api.session.client import APIClient
from clearml.backend_api.session import Session


TEMPLATE_DIR = Path(__file__).parent / "templates"
jinja_env = Environment(loader=FileSystemLoader(TEMPLATE_DIR))


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


def run_once(
    queue_name: str,
    extra_env_keys: list[str],
    max_jobs: int,
    config_file: str,
    account: str,
    running_jobs: int,
) -> list[dict]:
    """Single iteration. Returns list of sbatch scripts to submit."""

    if running_jobs >= max_jobs:
        print(f"[INFO] At max jobs ({max_jobs}), waiting...", file=sys.stderr)
        return []

    client = APIClient()

    queues = client.queues.get_all(name=queue_name)
    if not queues:
        raise ValueError(f"Queue '{queue_name}' not found")
    queue_id = queues[0].id

    num_entries = client.queues.get_num_entries(queue=queue_id).num
    if num_entries == 0:
        print("[INFO] Queue empty, waiting...", file=sys.stderr)
        return []

    print(f"[INFO] {num_entries} tasks queued, processing...", file=sys.stderr)

    scripts = []
    for _ in range(num_entries):
        if running_jobs + len(scripts) >= max_jobs:
            print("[INFO] Hit max jobs during burst", file=sys.stderr)
            break

        response = client.queues.get_next_task(queue=queue_id)
        if not response.entry:
            break

        task_id = response.entry.task
        task = Task.get_task(task_id=task_id)

        # Debug output
        print(f"[DEBUG] task_id={task_id}", file=sys.stderr)
        print(f"[DEBUG] memory={task.get_parameter('slurm/memory')}", file=sys.stderr)
        print(f"[DEBUG] time={task.get_parameter('slurm/time')}", file=sys.stderr)
        print(f"[DEBUG] cpu={task.get_parameter('slurm/cpu')}", file=sys.stderr)
        print(f"[DEBUG] gpu={task.get_parameter('slurm/gpu')}", file=sys.stderr)
        print(f"[DEBUG] log_dir={task.get_parameter('slurm/log_dir')}", file=sys.stderr)

        script = build_sbatch_script(task, task_id, config_file, account, extra_env_keys)
        scripts.append({"task_id": task_id, "script": script})
        print(f"[INFO] Prepared task {task_id}", file=sys.stderr)

    return scripts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", required=True)
    parser.add_argument("--envs", default="")
    parser.add_argument("--max_jobs", type=int, required=True)
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--account", default="")
    parser.add_argument("--running-jobs", type=int, required=True)
    args = parser.parse_args()

    env_keys = [e.strip() for e in args.envs.split(",") if e.strip()]

    try:
        scripts = run_once(
            args.queue,
            env_keys,
            args.max_jobs,
            args.config_file,
            args.account,
            args.running_jobs,
        )
        print(json.dumps(scripts))
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        print("[]")


if __name__ == "__main__":
    main()
