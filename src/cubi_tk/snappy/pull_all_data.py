"""``cubi-tk snappy pull-all-data``: pull all data from SODAR iRODS to SNAPPY dataset directory.
More Information
----------------
- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-all-data --help`` for more information.
- `SNAPPY Pipeline Documentation <https://snappy-pipeline.readthedocs.io/en/latest/>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
import typing

from loguru import logger

from ..sodar import pull_raw_data as sodar_pull_raw_data


class PullAllDataCommand:
    """Implementation of the ``snappy pull-all-data`` command."""

    def __init__(self, args: argparse.Namespace):
        #: Command line arguments.
        self.args = args

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
        return cls(args).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the download."""
        logger.info("=> will download to {}", self.args.output_directory)
        logger.info("Using cubi-tk sodar pull-raw-data to actually download data")
        res = sodar_pull_raw_data.PullRawDataCommand(
            self.args
        ).execute()

        if res:
            logger.error("cubi-tk sodar pull-all-data failed")
        else:
            logger.info("All done. Have a nice day!")
        return res


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-all-data``."""
    return PullAllDataCommand.setup_argparse(parser)
