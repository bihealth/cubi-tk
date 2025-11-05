"""``cubi-tk sodar upload-sheet``: upload ISA-tab to SODAR."""

import argparse
import contextlib
import itertools
from pathlib import Path
import pathlib
import typing

from loguru import logger

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi


from .. import isa_support


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
        parser.add_argument("input_investigation_file", help="Path to input investigation file.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("sodar_cmd", None)
        return cls(argparse.Namespace(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        logger.info("Starting cubi-tk sodar upload-sheet")
        sodar_api = SodarApi(self.args, with_dest=True)
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
        files_dict = {}
        with contextlib.ExitStack() as stack:
            for no, path in enumerate(file_paths):
                p = pathlib.Path(path)
                files_dict["file_%d" % no] = (p.name, stack.enter_context(p.open("rt")))

            sodar_api.post_samplesheet_import(files_dict=files_dict)

        logger.info("All done. Have a nice day!")
        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar upload-sheet``."""
    return UploadSheetCommand.setup_argparse(parser)
