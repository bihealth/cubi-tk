"""``cubi-tk isa-tab validate``: validation of ISA-tab files."""

import argparse
import typing

from altamisa.apps import isatab_validate
from logzero import logger


class ValidateIsaTabCommand:
    """Implementation of the ``validate`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="isa_tab_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--show-duplicate-warnings",
            dest="show_duplicate_warnings",
            action="store_true",
            help=(
                "Show duplicated warnings, i.e. with same message and same category (False by default)"
            ),
        )
        parser.set_defaults(no_warnings=False)
        parser.add_argument(
            "input_investigation_file",
            metavar="investigation.tsv",
            type=argparse.FileType("rt"),
            help="Path to ISA-tab investigation file.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, _args):
        """Called for checking arguments, override to change behaviour."""
        return 0

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk isa-tab validate")
        logger.info("  args: %s", self.args)

        return int(isatab_validate.run(self.args) is not None)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk isa-tab itransfer-raw-data``."""
    return ValidateIsaTabCommand.setup_argparse(parser)
