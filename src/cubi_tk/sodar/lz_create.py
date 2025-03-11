"""``cubi-tk sodar landing-zone-create`` command line program
"""

import argparse
import json
import typing

import cattr
from loguru import logger
from sodar_cli import api

from cubi_tk.parsers import check_args_sodar_config_parser, print_args


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
            "--assay-uuid", default=None, help="UUID of assay to create landing zone for."
        )

        parser.add_argument(
            "--format",
            dest="format_string",
            default=None,
            help="Format string for printing, e.g. %%(uuid)s",
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

        res, args = check_args_sodar_config_parser(args)

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the landing zone creation."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar landing-zone-create")
        print_args(self.args)

        existing_lzs = sorted(
            api.landingzone.list_(
                sodar_url=self.args.sodar_server_url,
                sodar_api_token=self.args.sodar_api_token,
                project_uuid=self.args.project_uuid,
            ),
            key=lambda lz: lz.date_modified,
        )
        existing_lzs = list(filter(lambda lz: lz.status == "ACTIVE", existing_lzs))
        if existing_lzs and self.args.unless_exists:
            lz = existing_lzs[-1]
        else:
            #TODO: make sure assay_uuid is not none
            lz = api.landingzone.create(
                sodar_url=self.args.sodar_server_url,
                sodar_api_token=self.args.sodar_api_token,
                project_uuid=self.args.project_uuid,
                assay_uuid=self.args.assay_uuid,
            )

        values = cattr.unstructure(lz)
        if self.args.format_string:
            print(self.args.format_string.replace(r"\t", "\t") % values)
        else:
            print(json.dumps(values))

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar landing-zone-create``."""
    return CreateLandingZoneCommand.setup_argparse(parser)
