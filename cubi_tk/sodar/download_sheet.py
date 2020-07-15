"""``cubi-tk sodar download-sheet``: download ISA-tab from SODAR."""

import argparse
import os
import typing
from pathlib import Path

import attr
from logzero import logger

from . import api
from ..common import overwrite_helper, load_toml_config
from ..exceptions import OverwriteRefusedException


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the download sheet command."""

    config: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    makedirs: bool
    overwrite: bool
    dry_run: bool
    yes: bool
    show_diff: bool
    show_diff_side_by_side: bool
    project_uuid: str
    output_dir: str


class DownloadSheetCommand:
    """Implementation of the ``download-sheet`` command."""

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

        logger.info("Starting cubi-tk sodar download-sheet")
        logger.info("  config: %s", self.config)

        out_path = Path(self.config.output_dir)
        if not out_path.exists() and self.config.makedirs:
            out_path.mkdir(parents=True)

        isa_dict = api.samplesheets.get(
            sodar_url=self.config.sodar_url,
            sodar_api_token=self.config.sodar_api_token,
            project_uuid=self.config.project_uuid,
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
            if self.config.verbose:
                logger.exception("%e", e)
            logger.error("%s", e)
            return 1
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
            answer_yes=self.config.yes,
            show_diff=self.config.show_diff,
            show_diff_side_by_side=self.config.show_diff_side_by_side,
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar download-sheet``."""
    return DownloadSheetCommand.setup_argparse(parser)
