"""``cubi-tk sodar landing-zone-list`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger

from cubi_tk.parsers import check_args_global_parser, print_args
from cubi_tk.sodar_api import LANDING_ZONE_STATES, SodarApi


class ListLandingZoneCommand:
    """Implementation of the ``landing-zone-list`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--unless-exists",
            default=False,
            dest="unless_exists",
            action="store_true",
            help="If there already is a landing zone in the current project then use this one",
        )

        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )

        parser.add_argument(
            "--format",
            dest="format_string",
            default=None,
            help="Format string for printing, e.g. %%(uuid)s",
        )

        parser.add_argument(
            "--filter-status",
            dest="filter_status",
            default=LANDING_ZONE_STATES,
            action="append",
            choices=LANDING_ZONE_STATES,
            help="Filter landing zone by status. Defaults to listing all.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        res, args = check_args_global_parser(args, with_dest=True)

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone listing."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar landing-zone-list")
        sodar_api = SodarApi(self.args, with_dest=True)
        print_args(self.args)

        existing_lzs = sodar_api.get_landingzone_list(filter_for_state = self.args.filter_status)
        for lz in existing_lzs:
            values = cattr.unstructure(lz)
            if self.args.format_string:
                print(self.args.format_string.replace(r"\t", "\t") % values)
            else:
                print(json.dumps(values))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-list``."""
    return ListLandingZoneCommand.setup_argparse(parser)
