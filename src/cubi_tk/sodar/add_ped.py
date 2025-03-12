"""``cubi-tk sodar add-ped``: augment ISA-tab with PED file."""

import argparse
import os
import pathlib
import tempfile
import typing

from loguru import logger

from cubi_tk.parsers import print_args

from ..isa_tab.add_ped import AddPedIsaTabCommand
from .download_sheet import DownloadSheetCommand
from .upload_sheet import UploadSheetCommand


class AddPedCommand:
    """Implementation of the ``add-ped`` command."""

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

        parser.add_argument(
            "--sample-name-normalization",
            default="snappy",
            choices=("snappy", "none"),
            help="Normalize sample names, default: snappy, choices: snappy, none",
        )

        parser.add_argument(
            "--yes", default=False, action="store_true", help="Assume all answers are yes."
        )

        parser.add_argument("--batch-no", default=".", help="Value to set as the batch number.")
        parser.add_argument(
            "--library-type",
            default="WES",
            choices=("WES", "WGS", "Panel_seq"),
            help="The library type.",
        )
        parser.add_argument(
            "--library-layout",
            default="PAIRED",
            choices=("SINGLE", "PAIRED"),
            help="The library layout.",
        )
        parser.add_argument("--library-kit", default="", help="The library kit used.")
        parser.add_argument(
            "--library-kit-catalogue-id", default="", help="The library kit catalogue ID."
        )
        parser.add_argument(
            "--platform", default="ILLUMINA", help="The string to use for the platform"
        )
        parser.add_argument(
            "--instrument-model", default="", help="The string to use for the instrument model"
        )

        parser.set_defaults(no_warnings=False)
        parser.add_argument(
            "input_ped_file",
            metavar="pedigree.ped",
            type=lambda x, parser=parser: cls.validate_pedigree_file(parser, x),
            help="Path to PLINK PED file with records to add.",
        )

    @classmethod
    def validate_pedigree_file(cls, parser, path_to_file):
        """Validate pedigree file

        :param parser: Argument parser.
        :type parser: argparse.ArgumentParser

        :param path_to_file: Path to pedigree file being checked.
        :type path_to_file: str

        :return: Returns inputted path if file exists and has 'rt' permissions.
        :raises PermissionError: if file doesn't have 'rt' permissions.
        """
        if not os.path.exists(path_to_file):
            parser.error(f"The provided pedigree file does not exist: {path_to_file}")
        try:
            with open(path_to_file, "rt"):
                pass
        except PermissionError as e:
            raise PermissionError(
                f"The provided file has invalid permissions: {path_to_file}"
            ) from e
        return path_to_file

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
        logger.info("Starting cubi-tk sodar add-ped")
        print_args(self.args)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(str(tmpdir))

            logger.info("-- downloading sample sheet --")
            dl_res = DownloadSheetCommand(
                self.args
            ).execute()
            if dl_res != 0:
                logger.error("-- downloading sheet failed --")
                return 1
            else:
                logger.info("-- downloading sheet succeeded --")

            logger.info("-- updating sample sheet --")
            self.args["input_investigation_file"] = str(tmp_path / next(tmp_path.glob("i_*")))
            print_args(self.args)

            add_res = AddPedIsaTabCommand(
                self.args
            ).execute()
            if add_res != 0:
                logger.error("-- updating sheet failed --")
                return 1
            else:
                logger.info("-- updating sheet succeeded --")

            logger.info("-- uploading sample sheet --")
            ul_res = UploadSheetCommand(
                self.args
            ).execute()
            if ul_res != 0:
                logger.error("-- uploading sheet failed --")
                return 1
            else:
                logger.info("-- uploading sheet succeeded --")

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar add-ped``."""
    return AddPedCommand.setup_argparse(parser)
