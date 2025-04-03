"""``cubi-tk sodar landing-zone-move`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

class MoveLandingZoneCommand:
    """Implementation of the ``landing-zone-move`` command."""

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

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()


    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone moving."""
        sodar_api = SodarApi(self.args)
        logger.info("Starting cubi-tk sodar landing-zone-move")
        print_args(self.args)

        new_lz_uuid = sodar_api.post_landingzone_submit_move(lz_uuid=self.args.landing_zone_uuid)
        if new_lz_uuid is None:
            return 1
        landing_zone = sodar_api.get_landingzone_retrieve(lz_uuid=new_lz_uuid)
        values = cattr.unstructure(landing_zone)
        if self.args.format_string:
            print(self.args.format_string.replace(r"\t", "\t") % values)
        else:
            print(json.dumps(values))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-move``."""
    return MoveLandingZoneCommand.setup_argparse(parser)
