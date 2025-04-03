"""``cubi-tk sodar landing-zone-validate`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi



class ValidateLandingZoneCommand:
    """Implementation of the ``landing-zone-validate`` command."""

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
        return cls(args).execute()  # pragma: nocover


    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone validation."""
        sodar_api = SodarApi(self.args)
        logger.info("Starting cubi-tk sodar landing-zone-validate.")
        print_args(self.args)

        lz_uuid = sodar_api.post_landingzone_submit_validate(lz_uuid=self.args.landing_zone_uuid)
        if lz_uuid is None:
            return 1
        landing_zone = sodar_api.get_landingzone_retrieve(lz_uuid=lz_uuid)
        values = cattr.unstructure(landing_zone)
        if self.args.format_string:
            logger.info("Formatted server response:")
            logger.info(self.args.format_string.replace(r"\t", "\t") % values)
        else:
            logger.info("Server response:")
            logger.info(json.dumps(values))
        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:  # pragma: nocover
    """Setup argument parser for ``cubi-tk sodar landing-zone-validate``."""
    return ValidateLandingZoneCommand.setup_argparse(parser)
