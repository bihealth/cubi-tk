"""``cubi-tk sodar upload-sheet``: upload ISA-tab to SODAR."""

import argparse
import os
import itertools
import typing
from pathlib import Path

import attr
from logzero import logger

from . import api
from .. import isa_support
from ..common import overwrite_helper, load_toml_config
from ..exceptions import OverwriteRefusedException


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the upload sheet command."""

    config: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    project_uuid: str
    input_investigation_file: str


class UploadSheetCommand:
    """Implementation of the ``upload-sheet`` command."""

    def __init__(self, config: Config):
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
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        toml_config = load_toml_config(self.config)
        if not self.config.sodar_url:
            self.config = attr.evolve(
                self.config, sodar_url=toml_config.get("global", {}).get("sodar_server_url")
            )
        if not self.config.sodar_api_token:
            self.config = attr.evolve(
                self.config, sodar_api_token=toml_config.get("global", {}).get("sodar_api_token")
            )

        logger.info("Starting cubi-tk sodar upload-sheet")
        logger.info("  config: %s", self.config)

        i_path = Path(self.config.input_investigation_file)
        if not i_path.exists():
            logger.error("Path does not exist: %s", i_path)
            return 1

        isa_data = isa_support.load_investigation(self.config.input_investigation_file)
        i_path = Path(self.config.input_investigation_file)
        file_paths = [i_path]
        for name in itertools.chain(isa_data.studies, isa_data.assays):
            file_paths.append(i_path.parent / name)

        logger.info("Uploading files: \n%s", "\n".join(map(str, file_paths)))

        api.samplesheets.upload(
            sodar_url=self.config.sodar_url,
            sodar_api_token=self.config.sodar_api_token,
            project_uuid=self.config.project_uuid,
            file_paths=file_paths,
        )

        logger.info("All done. Have a nice day!")
        return 0

    def _write_file(self, out_path, file_name, text):
        out_path = out_path / file_name
        if out_path.exists() and not self.config.overwrite and not self.config.dry_run:
            raise OverwriteRefusedException(
                "Refusing to overwrite without --overwrite: %s" % out_path
            )
        logger.info(
            "%s %s", "Not writing (dry-run)" if self.config.dry_run else "Writing", out_path
        )
        overwrite_helper(
            out_path,
            text,
            do_write=not self.config.dry_run,
            show_diff=self.config.show_diff,
            show_diff_side_by_side=self.config.show_diff_side_by_side,
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar upload-sheet``."""
    return UploadSheetCommand.setup_argparse(parser)
