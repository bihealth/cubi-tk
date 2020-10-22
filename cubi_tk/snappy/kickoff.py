"""``cubi-tk snappy kickoff``: kickoff SNAPPY pipeline."""

import argparse
import os
import subprocess
import time
import typing

from logzero import logger
from toposort import toposort

from . import common

from cubi_tk.exceptions import ParseOutputException


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    logger.info("Try to find SNAPPY pipeline directory...")
    try:
        path = common.find_snappy_root_dir(args.path or os.getcwd(), common.DEPENDENCIES.keys())
    except common.CouldNotFindPipelineRoot:
        return 1

    # TODO: this assumes standard naming which is a limitation...
    logger.info("Looking for pipeline directories (assuming standard naming)...")
    logger.debug("Looking in %s", path)
    step_set = {name for name in common.DEPENDENCIES if (path / name).exists()}
    steps: typing.List[str] = []
    for names in toposort({k: set(v) for k, v in common.DEPENDENCIES.items()}):
        steps += [name for name in names if name in step_set]
    logger.info("Will run the steps: %s", ", ".join(steps))

    logger.info("Submitting with sbatch...")
    jids: typing.Dict[str, str] = {}
    for step in steps:
        path_cache = path / step / ".snappy_path_cache"
        if step == "ngs_mapping" and path_cache.exists():
            age_cache = time.time() - path_cache.stat().st_mtime
            max_age = 24 * 60 * 60  # 1d
            if age_cache > max_age:
                logger.info("Cache older than %d - purging", max_age)
                path_cache.unlink()
        dep_jids = [jids[dep] for dep in common.DEPENDENCIES[step] if dep in jids]
        cmd = ["sbatch"]
        if dep_jids:
            cmd += ["--dependency", "afterok:%s" % ":".join(map(str, dep_jids))]
        cmd += ["pipeline_job.sh"]
        logger.info("Submitting step %s: %s", step, " ".join(cmd))
        if args.dry_run:
            jid = "<%s>" % step
        else:
            stdout_raw = subprocess.check_output(cmd, cwd=str(path / step), timeout=args.timeout)
            stdout = stdout_raw.decode("utf-8")
            if not stdout.startswith("Submitted batch job "):
                raise ParseOutputException("Did not understand sbatch output: %s" % stdout)
            jid = stdout.split()[-1]
        logger.info(" => JID: %s", jid)
        jids[step] = jid

    return None


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-sheet``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform dry-run, do not do anything.",
    )

    parser.add_argument(
        "--timeout", default=10, type=int, help="Number of seconds to wait for commands."
    )

    parser.add_argument(
        "path",
        nargs="?",
        help="Path into SNAPPY directory (below a directory containing .snappy_pipeline).",
    )
