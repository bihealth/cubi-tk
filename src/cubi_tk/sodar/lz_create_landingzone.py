"""``cubi-tk sodar create-landingzone`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi


class CreateLandingZoneCommand:
    """Implementation of the ``landing-zone-create`` command."""

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
    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone creation."""

        logger.info("Starting cubi-tk sodar create-landingzone")
        sodar_api = SodarApi(self.args, with_dest=True)
        print_args(self.args)

        existing_lzs = sodar_api.get_landingzone_list(filter_for_state = ["ACTIVE"])
        if existing_lzs and self.args.unless_exists:
            lz = existing_lzs[-1]
        else:
            lz = sodar_api.post_landingzone_create()
            if lz is None:
                return 1

        values = cattr.unstructure(lz)
        if self.args.format_string:
            print(self.args.format_string.replace(r"\t", "\t") % values)
        else:
            print(json.dumps(values, indent=4))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-create``."""
    return CreateLandingZoneCommand.setup_argparse(parser)
