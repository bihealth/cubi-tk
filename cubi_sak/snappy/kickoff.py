"""``cubi-sak snappy kickoff``: kickoff SNAPPY pipeline."""

import argparse
import os
import pathlib
import subprocess
import typing

from logzero import logger
from toposort import toposort


#: Dependencies between the SNAPPY steps.
from cubi_sak.exceptions import ParseOutputException

DEPENDENCIES: typing.Dict[str, typing.Tuple[str, ...]] = {
    "ngs_mapping": (),
    "variant_calling": ("ngs_mapping",),
    "variant_export": ("variant_calling",),
    "targeted_cnv_calling": ("ngs_mapping",),
    "targeted_cnv_annotation": ("targeted_cnv_calling",),
    "targeted_cnv_export": ("targeted_cnv_annotation",),
}

#: Timeout
TIMEOUT = 5


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:
    logger.info("Try to find SNAPPY pipeline directory...")
    start_path = pathlib.Path(args.path or os.getcwd())
    for path in [start_path] + list(start_path.parents):
        logger.debug("Trying %s", path)
        if (path / ".snappy_pipeline").exists() or any(
            (path / name).exists() for name in DEPENDENCIES.keys()
        ):
            logger.info("Will start at %s", path)
            break
    else:
        logger.error("Could not find SNAPPY pipeline directories below %s", start_path)
        return 1

    # TODO: this assumes standard naming which is a limitation...
    logger.info("Looking for pipeline directories (assuming standard naming)...")
    step_set = {name for name in DEPENDENCIES if (path / name).exists()}
    steps: typing.List[str] = []
    for names in toposort({k: set(v) for k, v in DEPENDENCIES.items()}):
        steps += [name for name in names if name in step_set]
    logger.info("Will run the steps: %s", ", ".join(steps))

    logger.info("Submitting with qsub...")
    jids: typing.Dict[str, str] = {}
    for step in steps:
        dep_jids = [jids[dep] for dep in DEPENDENCIES[step] if dep in jids]
        cmd = ["qsub"]
        if dep_jids:
            cmd += ["-hold_jid", ",".join(map(str, dep_jids))]
        cmd += ["pipeline_job.sh"]
        logger.info("Submitting step %s: %s", step, " ".join(cmd))
        if args.dry_run:
            jid = "<%s>" % step
        else:
            stdout_raw = subprocess.check_output(cmd, cwd=str(path / step), timeout=TIMEOUT)
            stdout = stdout_raw.decode("utf-8")
            if not stdout.startswith("Your job "):
                raise ParseOutputException("Did not understand qsub output: %s" % stdout)
            jid = stdout.split()[2]
        logger.info(" => JID: %s", jid)
        jids[step] = jid

    return None


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak snappy pull-sheet``."""
    parser.add_argument("--hidden-cmd", dest="snappy_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform dry-run, do not do anything.",
    )

    parser.add_argument(
        "path",
        nargs="?",
        help="Path into SNAPPY directory (below a directory containing .snappy_pipeline).",
    )
