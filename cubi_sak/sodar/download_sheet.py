"""``cubi-sak sodar download-sheet``: download ISA-tab from SODAR."""

import argparse
import os
import typing
from pathlib import Path

from logzero import logger

from . import api
from ..common import overwrite_helper
from ..exceptions import OverwriteRefusedException


class DownloadSheetCommand:
    """Implementation of the ``download-sheet`` command."""

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
            "--sodar-auth-token",
            default=os.environ.get("SODAR_AUTH_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_AUTH_TOKEN environment variable.",
        )

        parser.add_argument(
            "--no-makedirs",
            default=True,
            dest="makedirs",
            action="store_true",
            help="Create output directories",
        )
        parser.add_argument(
            "--overwrite", default=False, action="store_true", help="Allow overwriting of files"
        )

        parser.add_argument(
            "--dry-run",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )
        parser.add_argument(
            "--show-diff",
            default=False,
            action="store_true",
            help="Show change when creating/updating sample sheets.",
        )
        parser.add_argument(
            "--show-diff-side-by-side",
            default=False,
            action="store_true",
            help="Show diff side by side instead of unified.",
        )

        parser.add_argument("project_uuid", help="UUID of project to download the ISA-tab for.")
        parser.add_argument("output_dir", help="Path to output directory to write the sheet to.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        # if os.path.exists(args.output_dir) and not args.overwrite:
        #     logger.error(
        #         "Output directory %s already exists. Use --overwrite to allow overwriting.",
        #         args.output_dir,
        #     )
        #     res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-sak sodar download-sheet")
        logger.info("  args: %s", self.args)

        out_path = Path(self.args.output_dir)
        if not out_path.exists() and self.args.makedirs:
            out_path.mkdir(parents=True)

        client = api.Client(self.args.sodar_url, self.args.sodar_auth_token, self.args.project_uuid)
        isa_dict = client.samplesheets.get()
        try:
            self._write_file(
                out_path, isa_dict["investigation"]["path"], isa_dict["investigation"]["tsv"]
            )
            for path, tsv in isa_dict["studies"].items():
                self._write_file(out_path, path, tsv["tsv"])
            for path, tsv in isa_dict["assays"].items():
                self._write_file(out_path, path, tsv["tsv"])
        except OverwriteRefusedException as e:
            if self.args.verbose:
                logger.exception("%e", e)
            logger.error("%s", e)
            return 1

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
    """Setup argument parser for ``cubi-sak sodar download-sheet``."""
    return DownloadSheetCommand.setup_argparse(parser)
