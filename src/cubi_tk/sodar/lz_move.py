"""``cubi-tk sodar landing-zone-move`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger
from sodar_cli import api

from cubi_tk.parsers import check_args_sodar_config_parser, print_args

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

        parser.add_argument("landing_zone_uuid", help="UUID of landing zone to move.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        res, args = check_args_sodar_config_parser(args)

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone moving."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar landing-zone-move")
        print_args(self.args)

        landing_zone = api.landingzone.submit_move(
            sodar_url=self.args.sodar_server_url,
            sodar_api_token=self.args.sodar_api_token,
            landingzone_uuid=self.args.landing_zone_uuid,
        )
        values = cattr.unstructure(landing_zone)
        if self.args.format_string:
            print(self.args.format_string.replace(r"\t", "\t") % values)
        else:
            print(json.dumps(values))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-move``."""
    return MoveLandingZoneCommand.setup_argparse(parser)
