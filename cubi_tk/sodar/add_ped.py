"""``cubi-tk sodar add-ped``: augment ISA-tab with PED file."""

import argparse
import os
import pathlib
import tempfile
import typing

import attr
from logzero import logger

from .download_sheet import DownloadSheetCommand, Config as DownloadSheetConfig
from .upload_sheet import UploadSheetCommand, Config as UploadSheetConfig
from ..isa_tab.add_ped import AddPedIsaTabCommand, Config as AddPedIsaTabCommandConfig


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the download sheet command."""

    config: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    dry_run: bool
    show_diff: bool
    show_diff_side_by_side: bool
    sample_name_normalization: str
    batch_no: str
    yes: bool
    library_type: str
    library_layout: str
    library_kit: str
    library_kit_catalogue_id: str
    instrument_model: str
    no_warnings: bool
    platform: str
    project_uuid: str
    input_ped_file: str


class AddPedCommand:
    """Implementation of the ``add-ped`` command."""

    def __init__(self, config):
        #: Command line arguments.
        self.config = config

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
        parser.add_argument("project_uuid", help="UUID of project to download the ISA-tab for.")
        parser.add_argument(
            "input_ped_file",
            metavar="pedigree.ped",
            type=argparse.FileType("rt"),
            help="Path to PLINK PED file with records to add.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("sodar_cmd", None)
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        logger.info("Starting cubi-tk sodar add-ped")
        logger.info("  config: %s", self.config)

        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = pathlib.Path(str(tmpdir))

            logger.info("-- downloading sample sheet --")
            dl_res = DownloadSheetCommand(
                DownloadSheetConfig(
                    config=self.config.config,
                    verbose=self.config.verbose,
                    sodar_server_url=self.config.sodar_server_url,
                    sodar_url=self.config.sodar_url,
                    sodar_api_token=self.config.sodar_api_token,
                    makedirs=False,
                    overwrite=False,
                    dry_run=self.config.dry_run,
                    show_diff=self.config.show_diff,
                    show_diff_side_by_side=self.config.show_diff_side_by_side,
                    project_uuid=self.config.project_uuid,
                    output_dir=str(tmp_path),
                    yes=True,
                )
            ).execute()
            if dl_res != 0:
                logger.error("-- downloading sheet failed --")
                return 1
            else:
                logger.info("-- downloading sheet succeeeded --")

            logger.info("-- updating sample sheet --")
            add_res = AddPedIsaTabCommand(
                AddPedIsaTabCommandConfig(
                    config=self.config.config,
                    verbose=self.config.verbose,
                    sodar_server_url=self.config.sodar_server_url,
                    sodar_api_token=self.config.sodar_api_token,
                    no_warnings=self.config.no_warnings,
                    sample_name_normalization=self.config.sample_name_normalization,
                    yes=self.config.yes,
                    dry_run=self.config.dry_run,
                    library_type=self.config.library_type,
                    library_layout=self.config.library_layout,
                    library_kit=self.config.library_kit,
                    library_kit_catalogue_id=self.config.library_kit_catalogue_id,
                    platform=self.config.platform,
                    instrument_model=self.config.instrument_model,
                    batch_no=self.config.batch_no,
                    show_diff=self.config.show_diff,
                    show_diff_side_by_side=self.config.show_diff_side_by_side,
                    input_investigation_file=str(tmp_path / next(tmp_path.glob("i_*"))),
                    input_ped_file=self.config.input_ped_file,
                )
            ).execute()
            if add_res != 0:
                logger.error("-- updating sheet failed --")
                return 1
            else:
                logger.info("-- updating sheet succeeeded --")

            logger.info("-- uploading sample sheet --")
            ul_res = UploadSheetCommand(
                UploadSheetConfig(
                    config=self.config.config,
                    verbose=self.config.verbose,
                    sodar_server_url=self.config.sodar_server_url,
                    sodar_url=self.config.sodar_url,
                    sodar_api_token=self.config.sodar_api_token,
                    project_uuid=self.config.project_uuid,
                    input_investigation_file=str(tmp_path / next(tmp_path.glob("i_*"))),
                )
            ).execute()
            if ul_res != 0:
                logger.error("-- uploading sheet failed --")
                return 1
            else:
                logger.info("-- uploading sheet succeeeded --")

        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar add-ped``."""
    return AddPedCommand.setup_argparse(parser)
