"""``cubi-tk sea-snap working-dir``: Create working directory for RNA-SeA-SnaP pipeline."""

import argparse
import os
import shutil
import time
import typing

from pathlib import Path
from logzero import logger

#: config files
CONFIGS = dict(DE="DE_config.yaml", mapping="mapping_config.yaml")

CLUSTER_CONFIG = "cluster_config.json"


def run(
    args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
) -> typing.Optional[int]:

    # find Sea-snap directory
    logger.info("Try to find RNA-SeA-SnaP pipeline directory...")
    start_path = Path(args.sea_snap_path)
    path = None
    for path in [start_path] + list(start_path.parents):
        logger.debug("Trying %s", path)
        if (path / "mapping_pipeline.snake").exists():
            logger.info("Will start at %s", path)
            break
    else:
        logger.error("Could not find RNA-SeA-SnaP pipeline directories below %s", start_path)
        return 1

    # create working directory
    working_dir = Path(time.strftime(args.dirname))
    try:
        working_dir.mkdir(parents=True)
        logger.info("Working directory %s created...", str(working_dir))
    except FileExistsError:
        logger.error("Error: directory %s already exists!", str(working_dir))
        return 1

    logger.info("Copy config files...")
    # paths of config files
    config_files = [path / val for key, val in CONFIGS.items() if key in args.configs]

    # copy config files
    for configf in config_files:
        shutil.copy(str(configf), str(working_dir / configf.name))
        logger.debug("%s copied.", str(configf))

    cl_config = path / CLUSTER_CONFIG
    shutil.copy(str(cl_config), str(working_dir / cl_config.name))
    logger.debug("%s copied.", str(cl_config))

    # symlink to wrapper
    (working_dir / "sea-snap").symlink_to(path / "sea-snap.py")
    logger.debug("Symlink to %s created.", str(path / "sea-snap.py"))

    return None


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap working-dir``."""

    parser.add_argument("--hidden-cmd", dest="sea_snap_cmd", default=run, help=argparse.SUPPRESS)

    parser.add_argument(
        "--dry-run",
        "-n",
        default=False,
        action="store_true",
        help="Perform dry-run, do not do anything.",
    )

    parser.add_argument(
        "--dirname",
        "-d",
        default="results_%Y_%m_%d/",
        help="Name of the working directory to create (default: 'results_YEAR_MONTH_DAY/').",
    )

    parser.add_argument(
        "--configs",
        "-c",
        nargs="+",
        default=["mapping", "DE"],
        choices=["mapping", "DE"],
        help="Configs to be imported (default: all).",
    )

    parser.add_argument(
        "sea_snap_path",
        nargs="?",
        default=os.getcwd(),
        help="Path into RNA-SeA-SnaP directory (below a directory containing 'mapping_pipeline.snake').",
    )
