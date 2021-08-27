"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import attr
import datetime
import os
import re
import tempfile
import typing
import hashlib
from queue import Queue
from threading import Thread
import shlex

from pathlib import Path
from logzero import logger
from subprocess import SubprocessError, check_output

from . import common

DEFAULT_NUM_TRANSFERS = 8


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for ingest-meta."""

    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    dry_run: bool
    temp_dir: str
    assay_type: str
    destination: str


class DkfzIngestMetaCommand(common.DkfzCommandBase):
    """Implementation of dkfz ingest-meta command for raw data."""

    command_name = "ingest-meta"
    step_name = "raw_data"

    def __init__(self, config: Config):
        super().__init__(config)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

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
            "--temp-dir",
            default=os.environ.get("TMPDIR", None),
            help="Temporary directory. Defaults to TMPDIR environment variable.",
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't upload anything only display upload commands.",
        )

        parser.add_argument(
            "--assay-type", default="EXON", help="Assay type to upload to landing zone"
        )

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0
        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk org-raw check")
        logger.info("  args: %s", self.config)

        metas = self.read_metas()
        self.map_ids(metas)

        tempdir = self._create_tempdir()

        files_to_upload = list()
        for meta in metas:
            if self.config.assay_type not in meta.content.keys():
                continue
            files_to_upload.append(meta.filename)
            md5_file = DkfzIngestMetaCommand._save_md5_to_temp(meta.filename, tempdir.name)
            if not md5_file:
                return -1
            files_to_upload.append(md5_file)
        if len(files_to_upload) == 0:
            logger.warning("No fastq file to upload")
            return 0

        commands = self._build_commands(
            files_to_upload=files_to_upload,
            landing_zone=self.config.destination
        )

        self._execute_commands(commands)

        return 0

    def _create_tempdir(self):
        tempdir = tempfile.TemporaryDirectory(dir=self.config.temp_dir)
        logger.info("Created temporary directory {}".format(tempdir))
        return tempdir

    @staticmethod
    def _compute_md5_checksum(filename, buffer_size=1048576):
        logger.info("Computing md5 hash for {}".format(filename))
        hash = None
        with open(filename, "rb") as f:
            hash = hashlib.md5()
            chunk = f.read(buffer_size)
            while chunk:
                hash.update(chunk)
                chunk = f.read(buffer_size)
        return hash.hexdigest()

    @staticmethod
    def _save_md5_to_temp(filename, tempdir):
        md5 = DkfzIngestMetaCommand._compute_md5_checksum(filename)
        filename = os.path.basename(filename)
        destination = None
        try:
            destination = "{}/{}.md5".format(tempdir, filename)
            with open(destination, "w") as f:
                print("{}  {}".format(md5, filename), file=f)
        except:
            logger.error("Can't create temporary md5 checksum for {}".format(filename))
            return None
        logger.info("Saving checksum of metafile {} to temporary file {}".format(filename, destination))
        return destination

    def _build_commands(
        self,
        files_to_upload,
        landing_zone,
    ):
        iput_cmd = ["iput", "-aK"]
        commands = []
        cmd = ["imkdir", "-p", "{landing_zone}/MiscFiles/DKFZ_meta".format(landing_zone=landing_zone)]
        commands.append(cmd)
        for filename in files_to_upload:
            target_path = "{landing_zone}/MiscFiles/DKFZ_meta/{filename}".format(
                landing_zone=landing_zone,
                filename=os.path.basename(filename)
            )
            cmd = iput_cmd + [str(filename), target_path]
            commands.append(cmd)
        return commands

    def _execute_commands(self, commands):
        for cmd in commands:
            if self.config.dry_run:
                print(" ".join(cmd))
                continue
            try:
                cmd_str = " ".join(map(shlex.quote, cmd))
                logger.info("Executing %s", cmd_str)
                print(cmd)
                print(cmd_str)
                check_output(cmd)
            except SubprocessError as e:  # pragma: nocover
                logger.error("Problem executing command: %s", e)
                return 1
        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz ingest-meta``."""
    return DkfzIngestMetaCommand.setup_argparse(parser)
