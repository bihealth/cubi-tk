"""``cubi-tk sodar landing-zone-validate`` command line program
"""

import argparse
import json
import os
import typing

import cattr
from loguru import logger
from sodar_cli import api

from ..common import load_toml_config



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

        parser.add_argument("landing_zone_uuid", help="UUID of landing zone to validate.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()  # pragma: nocover

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        toml_config = load_toml_config(args)
        args.sodar_server_url = args.sodar_server_url or toml_config.get("global", {}).get("sodar_server_url")
        args.sodar_api_token = args.sodar_api_token or toml_config.get("global", {}).get(
            "sodar_api_token"
        )

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone validation."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar landing-zone-validate.")
        logger.debug("args: {}", self.args)

        landing_zone = api.landingzone.submit_validate(
            sodar_url=self.args.sodar_server_url,
            sodar_api_token=self.args.sodar_api_token,
            landingzone_uuid=self.args.landing_zone_uuid,
        )
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
