"""``cubi-tk sodar landing-zone-list`` command line program
"""

import argparse
import json
import os
import typing

import cattr
from loguru import logger
from sodar_cli import api

from ..common import load_toml_config

# TODO: Obtain from somewhere else, e.g. sodar-cli or sodar API or sodar-core or …
LANDING_ZONE_STATES = ["ACTIVE", "FAILED", "VALIDATING"]


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

        parser.add_argument("project_uuid", help="UUID of project to create the landing zone in.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

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
        """Execute the landing zone listing."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar landing-zone-list")
        logger.info("  args: {}", self.args)

        existing_lzs = sorted(
            api.landingzone.list_(
                sodar_url=self.args.sodar_server_url,
                sodar_api_token=self.args.sodar_api_token,
                project_uuid=self.args.project_uuid,
            ),
            key=lambda lz: lz.date_modified,
        )
        for lz in existing_lzs:
            if lz.status not in self.args.filter_status:
                continue
            values = cattr.unstructure(lz)
            if self.args.format_string:
                print(self.args.format_string.replace(r"\t", "\t") % values)
            else:
                print(json.dumps(values))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-list``."""
    return ListLandingZoneCommand.setup_argparse(parser)
