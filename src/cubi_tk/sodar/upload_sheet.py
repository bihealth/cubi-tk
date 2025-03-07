"""``cubi-tk sodar upload-sheet``: upload ISA-tab to SODAR."""

import argparse
import itertools
from pathlib import Path
import typing

from loguru import logger
from sodar_cli import api

from cubi_tk.parsers import check_args_sodar_config_parser, print_args

from .. import isa_support
from ..common import overwrite_helper
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

        parser.add_argument("project_uuid", help="UUID of project to upload the ISA-tab for.")
        parser.add_argument("input_investigation_file", help="Path to input investigation file.")

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
        _, self.args = check_args_sodar_config_parser(self.args)

        logger.info("Starting cubi-tk sodar upload-sheet")
        print_args(self.args)

        i_path = Path(self.args.input_investigation_file)
        if not i_path.exists():
            logger.error("Path does not exist: {}", i_path)
            return 1

        isa_data = isa_support.load_investigation(self.args.input_investigation_file)
        i_path = Path(self.args.input_investigation_file)
        file_paths = [i_path]
        for name in itertools.chain(isa_data.studies, isa_data.assays):
            file_paths.append(i_path.parent / name)

        logger.info("Uploading files: \n{}", "\n".join(map(str, file_paths)))

        api.samplesheet.upload(
            sodar_url=self.args.sodar_server_url,
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
        logger.info(
            "{} {}", "Not writing (dry-run)" if self.args.dry_run else "Writing", out_path
        )
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
