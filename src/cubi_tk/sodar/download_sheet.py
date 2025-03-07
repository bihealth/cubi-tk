"""``cubi-tk sodar download-sheet``: download ISA-tab from SODAR."""

import argparse
from pathlib import Path
import typing

from loguru import logger
from sodar_cli import api

from cubi_tk.parsers import check_args_sodar_config_parser, print_args

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
            "--yes", default=False, action="store_true", help="Assume all answers are yes."
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )
        parser.add_argument(
            "--show-diff",
            "-D",
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
        args = vars(args)
        args.pop("cmd", None)
        args.pop("sodar_cmd", None)
        return cls(args).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        _any_error, self.args = check_args_sodar_config_parser(self.args)
        logger.info("Starting cubi-tk sodar download-sheet")
        print_args(self.args)

        out_path = Path(self.args.output_dir)
        if not out_path.exists() and self.args.makedirs:
            out_path.mkdir(parents=True)

        isa_dict = api.samplesheet.export(
            sodar_url=self.args.sodar_server_url,
            sodar_api_token=self.args.sodar_api_token,
            project_uuid=self.args.project_uuid,
        )
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
                logger.exception("{}", e)
            logger.error("{}", e)
            return 1
        return 0

    def _write_file(self, out_path, file_name, text):
        """Write file.

        :param out_path: Path to output directory.
        :type out_path: str

        :param file_name: File name as provided in SODAR. If input includes more than file name, extra information
        is removed. File should be created in output path root.
        Example: 'PROJECT/i_Investigation.txt' -> 'i_Investigation.txt'.
        :type file_name: str

        :param text: Text to be written in file.
        :type text: str
        """
        # Remove extra info - use basename only
        file_name = file_name.split("/")[-1]
        out_path = out_path / file_name
        if out_path.exists() and not self.args.overwrite and not self.args.dry_run:
            raise OverwriteRefusedException(
                "Refusing to overwrite without --overwrite: %s" % out_path
            )
        logger.info(
            "{} {}", "Not writing (dry-run)" if self.args.dry_run else "Writing", out_path
        )
        overwrite_helper(
            out_path,
            text,
            do_write=not self.args.dry_run,
            answer_yes=self.args.yes,
            show_diff=self.args.show_diff,
            show_diff_side_by_side=self.args.show_diff_side_by_side,
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar download-sheet``."""
    return DownloadSheetCommand.setup_argparse(parser)
