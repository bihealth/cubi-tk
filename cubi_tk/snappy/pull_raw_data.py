"""``cubi-tk snappy pull-raw-data``: pull raw data from SODAR iRODS to SNAPPY dataset directory.

More Information
----------------

- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-sheet --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import os
import pathlib
import typing

import attr
from logzero import logger
import yaml

from .common import find_snappy_root_dir
from ..sodar import pull_raw_data as sodar_pull_raw_data


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the pull-raw-data."""

    base_path: str
    config: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    overwrite: bool
    min_batch: int
    dry_run: bool
    irsync_threads: int
    yes: bool
    project_uuid: str
    assay: str
    samples: typing.List[str]


class PullRawDataCommand:
    """Implementation of the ``snappy pull-raw-data`` command."""

    def __init__(self, config: Config):
        #: Command line arguments.
        self.config = config

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path of project (contains '.snappy_pipeline/' etc.), spiders up from current "
                "work directory and falls back to current working directory by default."
            ),
        )

        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--overwrite", default=False, action="store_true", help="Allow overwriting of files"
        )
        parser.add_argument("--min-batch", default=0, type=int, help="Minimal batch number to pull")
        parser.add_argument("--samples", help="Optional list of samples to pull")

        parser.add_argument(
            "--yes", default=False, action="store_true", help="Assume all answers are yes."
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )
        parser.add_argument("--irsync-threads", help="Parameter -N to pass to irsync")

        parser.add_argument(
            "--assay", dest="assay", default=None, help="UUID of assay to create landing zone for."
        )

        parser.add_argument("project_uuid", help="UUID of project to download data for.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("snappy_cmd", None)
        args["base_path"] = find_snappy_root_dir(args["base_path"])
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the download."""
        logger.info("Loading configuration file and look for dataset")

        with (pathlib.Path(self.config.base_path) / ".snappy_pipeline" / "config.yaml").open(
            "rt"
        ) as inputf:
            config = yaml.safe_load(inputf)
        if "data_sets" not in config:
            logger.error(
                "Could not find configuration key %s in %s", repr("data_sets"), inputf.name
            )
            return 1
        data_set = {}
        for key, data_set in config["data_sets"].items():
            if (
                key == self.config.project_uuid
                or data_set.get("sodar_uuid") == self.config.project_uuid
            ):
                break
        else:  # no "break" out of for-loop
            logger.error(
                "Could not find dataset with key/sodar_uuid entry of %s", self.config.project_uuid
            )
            return 1
        if not data_set.get("search_paths"):
            logger.error("data set has no attribute %s", repr("search_paths"))
            return 1

        download_path = data_set["search_paths"][-1]
        logger.info("=> will download to %s", download_path)

        logger.info("Using cubi-tk sodar pull-raw-data to actually download data")
        res = sodar_pull_raw_data.PullRawDataCommand(
            sodar_pull_raw_data.Config(
                config=self.config.config,
                verbose=self.config.verbose,
                sodar_server_url=self.config.sodar_server_url,
                sodar_url=self.config.sodar_url,
                sodar_api_token=self.config.sodar_api_token,
                overwrite=self.config.overwrite,
                min_batch=self.config.min_batch,
                dry_run=self.config.dry_run,
                irsync_threads=self.config.irsync_threads,
                yes=self.config.yes,
                project_uuid=self.config.project_uuid,
                assay=self.config.assay,
                output_dir=download_path,
            )
        ).execute()

        if res:
            logger.error("cubi-tk sodar pull-raw-data failed")
        else:
            logger.info("All done. Have a nice day!")
        return res


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-raw-data``."""
    return PullRawDataCommand.setup_argparse(parser)
