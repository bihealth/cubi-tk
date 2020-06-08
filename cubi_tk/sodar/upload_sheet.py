"""``cubi-tk sodar upload-sheet``: upload ISA-tab to SODAR."""

import argparse
import os
import itertools
import typing
from pathlib import Path

from logzero import logger

from . import api
from .. import isa_support
from ..common import overwrite_helper, load_toml_config
from ..exceptions import OverwriteRefusedException


class UploadSheetCommand:
    """Implementation of the ``upload-sheet`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument("project_uuid", help="UUID of project to upload the ISA-tab for.")
        parser.add_argument("input_investigation_file", help="Path to input investigation file.")

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
        args.sodar_url = args.sodar_url or toml_config.get("global", {}).get("sodar_server_url")
        args.sodar_api_token = args.sodar_api_token or toml_config.get("global", {}).get(
            "sodar_api_token"
        )

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar upload-sheet")
        logger.info("  args: %s", self.args)

        i_path = Path(self.args.input_investigation_file)
        if not i_path.exists():
            logger.error("Path does not exist: %s", i_path)
            return 1

        isa_data = isa_support.load_investigation(self.args.input_investigation_file)
        i_path = Path(self.args.input_investigation_file)
        file_paths = [i_path]
        for name in itertools.chain(isa_data.studies, isa_data.assays):
            file_paths.append(i_path.parent / name)

        logger.info("Uploading files: \n%s", "\n".join(map(str, file_paths)))

        api.samplesheets.upload(
            sodar_url=self.args.sodar_url,
            sodar_api_token=self.args.sodar_api_token,
            project_uuid=self.args.project_uuid,
            file_paths=file_paths,
        )

        logger.info("All done. Have a nice day!")
        return 0

    def _write_file(self, out_path, file_name, text):
        out_path = out_path / file_name
        if out_path.exists() and not self.args.overwrite and not self.args.dry_run:
            raise OverwriteRefusedException(
                "Refusing to overwrite without --overwrite: %s" % out_path
            )
        logger.info("%s %s", "Not writing (dry-run)" if self.args.dry_run else "Writing", out_path)
        overwrite_helper(
            out_path,
            text,
            do_write=not self.args.dry_run,
            show_diff=self.args.show_diff,
            show_diff_side_by_side=self.args.show_diff_side_by_side,
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar upload-sheet``."""
    return UploadSheetCommand.setup_argparse(parser)
