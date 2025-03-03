"""``cubi-tk snappy kickoff``: kickoff SNAPPY pipeline."""

import argparse
import os
import subprocess
import time
import typing

from loguru import logger
from toposort import toposort

from cubi_tk.exceptions import ParseOutputException

from . import common
from .snappy_workflows import SnappyWorkflowManager


class SnappyMissingPackageException(Exception):
    def __str__(self):
        return "snappy-pipeline is not installed. This function will not work."


class SnappyMissingDependencyException(Exception):
    """Raised if dependencies of steps do not exist in the current workflow."""

    def __init__(
        self, step_name: str, step_dependencies: typing.List[str], existing_steps: typing.List[str]
    ):
        self.step_name = step_name
        self.step_dependencies = step_dependencies
        self.existing_steps = existing_steps

    def __str__(self):
        return f"{self.step_name} requires {self.step_dependencies}, but only {self.existing_steps} exist in workflow directory."


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    logger.info("Try to find SNAPPY pipeline directory...")
    try:
        path = common.find_snappy_root_dir(args.path or os.getcwd())
    except common.CouldNotFindPipelineRoot:
        return 1

    logger.info("Looking for pipeline directories (needs to contain snappy config.yaml)...")
    logger.debug("Looking in {}", path)

    manager = SnappyWorkflowManager.from_snappy()

    if manager is None:
        raise SnappyMissingPackageException

    step_dependencies = {}
    folder_steps = manager.get_snappy_step_directories(path)
    for step_name, step_path in folder_steps.items():
        dependencies = manager.get_workflow_step_dependencies(step_path)
        if not all(dep in folder_steps for dep in dependencies):
            raise SnappyMissingDependencyException(
                step_name, dependencies, list(folder_steps.keys())
            )

        step_dependencies[step_name] = dependencies

    steps: typing.List[str] = []
    for names in toposort({k: set(v) for k, v in step_dependencies.items()}):
        steps += names
    logger.info("Will run the steps: {}", ", ".join(steps))

    logger.info("Submitting with sbatch...")
    jids: typing.Dict[str, str] = {}

    for step in steps:
        step_path = folder_steps[step]
        path_cache = step_path / ".snappy_path_cache"
        if step == "ngs_mapping" and path_cache.exists():
            age_cache = time.time() - path_cache.stat().st_mtime
            max_age = 24 * 60 * 60  # 1d
            if age_cache > max_age:
                logger.info("Cache older than {} - purging", max_age)
                path_cache.unlink()
        dep_jids = [jids[dep] for dep in step_dependencies[step] if dep in jids]
        cmd = ["sbatch"]
        if dep_jids:
            cmd += ["--dependency", "afterok:" + ":".join(map(str, dep_jids))]
        cmd += ["pipeline_job.sh"]
        logger.info("Submitting step {} (./{}): {}", step, step_path.name, " ".join(cmd))
        if args.dry_run:
            jid = f"<{step}>"
        else:
            stdout_raw = subprocess.check_output(cmd, cwd=str(step_path), timeout=args.timeout)
            stdout = stdout_raw.decode("utf-8")
            if not stdout.startswith("Submitted batch job "):
                raise ParseOutputException("Did not understand sbatch output: {}".format(stdout))
            jid = stdout.split()[-1]
        logger.info(" => JID: {}", jid)
        jids[step] = jid

    return None


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy kickoff``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform dry-run, do not do anything.",
    )

    parser.add_argument(
        "--timeout",
        default=10,
        type=int,
        help="Number of seconds to wait for commands.",
    )

    parser.add_argument(
        "path",
        nargs="?",
        help="Path into SNAPPY directory (below a directory containing .snappy_pipeline).",
    )
