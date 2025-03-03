"""``cubi-tk snappy pull-all-data``: pull all data from SODAR iRODS to SNAPPY dataset directory.
More Information
----------------
- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-all-data --help`` for more information.
- `SNAPPY Pipeline Documentation <https://snappy-pipeline.readthedocs.io/en/latest/>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import os
import typing

import attr
from loguru import logger

from ..sodar import pull_raw_data as sodar_pull_raw_data


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the pull-all-data."""

    config: str
    verbose: bool
    sodar_server_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    overwrite: bool
    first_batch: int
    dry_run: bool
    irsync_threads: int
    yes: bool
    output_directory: str
    samples: typing.List[str]
    allow_missing: bool
    assay_uuid: str
    project_uuid: str


class PullAllDataCommand:
    """Implementation of the ``snappy pull-all-data`` command."""

    def __init__(self, config: Config):
        #: Command line arguments.
        self.config = config

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        #TODO: implement functionality for tsv-shortcut and last-batch
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--allow-missing",
            default=False,
            action="store_true",
            help="Allow missing data in assay",
        )
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
        
    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("snappy_cmd", None)
        args.pop("base_path", None)
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the download."""
        logger.info("=> will download to {}", self.config.output_directory)
        logger.info("Using cubi-tk sodar pull-raw-data to actually download data")
        res = sodar_pull_raw_data.PullRawDataCommand(
            sodar_pull_raw_data.Config(
                config=self.config.config,
                verbose=self.config.verbose,
                sodar_server_url=self.config.sodar_server_url,
                sodar_api_token=self.config.sodar_api_token,
                overwrite=self.config.overwrite,
                min_batch=self.config.first_batch,
                allow_missing=self.config.allow_missing,
                dry_run=self.config.dry_run,
                irsync_threads=self.config.irsync_threads,
                yes=self.config.yes,
                project_uuid=self.config.project_uuid,
                assay=self.config.assay_uuid,
                output_dir=self.config.output_directory,
            )
        ).execute()

        if res:
            logger.error("cubi-tk sodar pull-all-data failed")
        else:
            logger.info("All done. Have a nice day!")
        return res


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-all-data``."""
    return PullAllDataCommand.setup_argparse(parser)
