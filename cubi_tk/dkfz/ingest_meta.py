"""``cubi-tk dkfz ingest-meta``: transfer DKFZ metafiles into iRODS landing zone."""

import argparse
import attr
import datetime
import os
import re
import tempfile
import typing
import hashlib
import shlex

from logzero import logger
from subprocess import SubprocessError, check_output

from ..sodar.api import landing_zones

from . import common

DEFAULT_NUM_TRANSFERS = 8


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for ingest-meta."""

    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    dry_run: bool
    temp_dir: str
    download_report: str
    assay_type: str
    destination: str


class DkfzIngestMetaCommand(common.DkfzCommandBase):
    """Implementation of dkfz ingest-meta command for raw data."""

    command_name = "ingest-meta"
    step_name = "raw_data"

    META_PATTERN = re.compile("^(.*/)?([0-9]+)_meta.tsv$")

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
            "--extra-files",
            nargs="+",
            default=None,
            help="Upload additional file(s) to MiscFiles/DKFZ_upload/<date>",
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

        self._create_tempdir()

        files_to_upload = list()
        for meta in metas:
            if self.config.assay_type not in meta.content.keys():
                continue

            # Upload meta file
            files_to_upload += self._add_to_upload_list(meta.filename)

            # Upload id mapping table
            filename = os.path.join(self.tempdir, "mapping_table.txt")
            self.mapper.df.to_csv(filename, sep="\t", index=False)
            files_to_upload += self._add_to_upload_list(filename)

            m = DkfzIngestMetaCommand.META_PATTERN.match(meta.filename)
            if m:
                path = m.group(1) if m.group(1) else "./"
                ilse_nb = m.group(2)

                # Upload pdf report (present from 01/2018, otherwise sent by e-mail)
                pdf = path + ilse_nb + "_report.pdf"
                if os.path.exists(pdf) and os.path.isfile(pdf):
                    files_to_upload += self._add_to_upload_list(pdf)

                # Upload additional Excel files (must be downloaded separately from ILSe)
                pattern = re.compile("^" + ilse_nb + "-.+\\.xls$")
                xlss = list(
                    filter(
                        lambda x: x is not None,
                        [x if pattern.match(x) else None for x in os.listdir(path)],
                    )
                )
                for xls in xlss:
                    files_to_upload += self._add_to_upload_list(path + xls)

        if self.config.extra_files:
            theDate = datetime.date.today().strftime("%Y-%m-%d")
            for filename in self.config.extra_files:
                files_to_upload += self._add_to_upload_list(
                    filename, target_path="MiscFiles/DKFZ_upload/{}".format(theDate)
                )

        if len(files_to_upload) == 0:
            logger.warning("No file to upload")
            return 0

        targets = [x[1] for x in files_to_upload]
        if len(set(targets)) < len(targets):
            logger.error("Attempting to upload duplicate file(s)")
            return -1

        if "/" in self.config.destination:
            lz_irods_path = self.config.destination
        else:
            lz_irods_path = landing_zones.get(
                sodar_url=self.config.sodar_url,
                sodar_api_token=self.config.sodar_api_token,
                landing_zone_uuid=self.config.destination,
            ).irods_path
            logger.info("Target iRods path: %s", lz_irods_path)

        commands = self._build_commands(files_to_upload=files_to_upload, landing_zone=lz_irods_path)

        self._execute_commands(commands)

        return 0

    def _create_tempdir(self):
        self.tempdir = tempfile.mkdtemp(dir=self.config.temp_dir)
        logger.info("Created temporary directory {}".format(self.tempdir))

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

    def _save_md5_to_temp(self, filename):
        md5 = DkfzIngestMetaCommand._compute_md5_checksum(filename)
        filename = os.path.basename(filename)
        destination = "{}/{}.md5".format(self.tempdir, filename)
        with open(destination, "w") as f:
            print("{}  {}".format(md5, filename), file=f)
        logger.info(
            "Saving checksum of metafile {} to temporary file {}".format(filename, destination)
        )
        return destination

    def _add_to_upload_list(self, filename, target_path="MiscFiles/DKFZ_meta"):
        md5_file = self._save_md5_to_temp(filename)
        return [
            (filename, os.path.join(target_path, os.path.basename(filename))),
            (md5_file, os.path.join(target_path, os.path.basename(md5_file))),
        ]

    def _build_commands(self, files_to_upload, landing_zone):
        iput_cmd = ["iput", "-aK"]
        imkdir_cmd = ["imkdir", "-p"]

        commands = []

        dirs = set([os.path.dirname(x[1]) for x in files_to_upload])
        for d in dirs:
            cmd = imkdir_cmd + ["{landing_zone}/{path}".format(landing_zone=landing_zone, path=d)]
            commands.append(cmd)

        for filename in files_to_upload:
            target_path = "{landing_zone}/{filename}".format(
                landing_zone=landing_zone, filename=filename[1]
            )
            cmd = iput_cmd + [str(filename[0]), target_path]
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
