"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import attr
import datetime
import os
import re
import typing
import hashlib
from queue import Queue
from threading import Thread
from subprocess import check_output, SubprocessError
import shlex
 
from pathlib import Path
from logzero import logger

from . import common
from .DkfzMeta import DkfzMeta
from ..sodar.api import landing_zones

DEFAULT_NUM_TRANSFERS = 8

@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for ingest-fastq."""

    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    dry_run: bool
    num_parallel_transfers: int
    tsv_shortcut: str
    remote_dir_date: str
    remote_dir_pattern: str
    assay_type: str
    md5_check: bool
    destination: str

class DkfzIngestFastqCommand(common.DkfzCommandBase):
    """Implementation of dkfz ingest-fastq command for raw data."""

    command_name = "ingest-fastq"
    step_name = "raw_data"

    md5_pattern = re.compile("^[0-9a-f]{32} +(.+)$", re.IGNORECASE)
    filename_pattern = re.compile("^([A-Z0-9-]+)_R[12]\\.fastq\\.gz$")

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
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't upload anything only display upload commands.",
        )

        parser.add_argument(
            "--num-parallel-transfers",
            type=int,
            default=DEFAULT_NUM_TRANSFERS,
            help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
        )
        parser.add_argument(
            "--tsv-shortcut",
            default="cancer",
            choices=("germline", "cancer"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--remote-dir-date",
            default=datetime.date.today().strftime("%Y-%m-%d"),
            help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
        )
        parser.add_argument(
            "--remote-dir-pattern",
            default="{library_name}/%s/{date}" % cls.step_name,
            help="Pattern to use for constructing remote pattern",
        )

        parser.add_argument(
            "--assay-type",
            default="EXON",
            help="Assay type to upload to landing zone"
        )

        parser.add_argument(
            "--md5-check",
            action="store_true",
            help="Compute md5 checksums and verify them against metadata"
        )

        parser.add_argument(
            "destination", help="UUID or iRods path of landing zone to move to."
        )

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

        files_to_upload = list()
        for meta in metas:
            if not self.config.assay_type in meta.content.keys():
                continue
            prefix = Path(meta.filename).parent
            for md5, row in meta.content[self.config.assay_type].items():
                run_id = row.row["RUN_ID"]
                fastq  = row.row["FASTQ_FILE"]
                m = DkfzIngestFastqCommand.filename_pattern.match(fastq)
                if not m:
                    logger.error("Unexpected filename pattern for file {}, ignored".format(fastq))
                    continue
                lib_id = m.group(1)
                filename = prefix / "{}/{}/fastq/{}".format(run_id, lib_id, fastq)
                checkfile = prefix / "{}/{}/fastq/{}.md5sum".format(run_id, lib_id, fastq)
                if (not filename.exists()) or (not checkfile.exists()):
                    logger.error("Can't find fastq file {}".format(filename))
                    continue
                checksum = DkfzIngestFastqCommand._read_companion_md5(checkfile)
                if md5 != checksum:
                    logger.error("MD5 checksum in metafile & in {} are not equal".format(checksum))
                    continue
                files_to_upload.append((md5, filename, self.mapper.df.loc[md5,"Library Name"]))
        if len(files_to_upload) == 0:
            logger.warning("No fastq file to upload")
            return 0

        if self.config.md5_check:
            files_to_upload = DkfzIngestFastqCommand._verify_checksums(files_to_upload, threads=self.config.num_parallel_transfers)

        commands = DkfzIngestFastqCommand._build_commands(
            files_to_upload=files_to_upload,
            landing_zone=self.config.destination,
            date=self.config.remote_dir_date,
            pattern=self.config.remote_dir_pattern % self.step_name
        )

        DkfzIngestFastqCommand._execute_commands(commands)

        return 0

    @staticmethod
    def _compute_md5_checksum(filename, buffer_size=1048576):
        hash = None
        with open(filename, "rb") as f:
            hash = hashlib.md5()
            chunk = f.read(buffer_size)
            while chunk:
                hash.update(chunk)
                chunk = f.read(buffer_size)
        return hash.hexdigest()

    @staticmethod
    def _compute_md5_checksum_queue(checksums, queue, buffer_size=1048576):
        while True:
            filename = queue.get()
            checksums[filename] = DkfzIngestFastqCommand._compute_md5_checksum(filename, buffer_size=buffer_size)
            queue.task_done()

    @staticmethod
    def _compute_checksums(filenames, threads=None):
        checksums = {}
        if threads and threads > 1:
            queue = Queue()
            for f in filenames:
                queue.put(f)
            for i in range(threads):
                worker = Thread(target=DkfzIngestFastqCommand._compute_md5_checksum_queue, args=(checksums, queue))
                worker.setDaemon(True)
                worker.start()
            queue.join()
        else:
            for f in filenames:
                checksums[f] = DkfzIngestFastqCommand.compute_md5_checksum(f)
        return checksums

    @staticmethod
    def _read_companion_md5(filename, ext=".md5sum"):
        lines = None
        with open(filename + ext, "r") as f:
            lines = f.readlines()
        if len(lines) != 1:
            raise ValueError("Empty or multiple lines in checksum file {}".format(filename + ext))
        m = DkfzIngestFastqCommand.md5_pattern.match(lines[0])
        if not m:
            raise ValueError("MD5 checksum {} in file {} doesn't match expected pattern".format(lines[0], filename + ext))
        if m.group(2) != Path(filename).name:
            raise ValueError("Checkum file {} doesn't report matching file (contents is {})".format(filename + ext, lines[0]))
        return m.group(1).lower()

    @staticmethod
    def _verify_checksums(files_to_upload, threads=1):
        filenames = set()
        for file_to_upload in files_to_upload:
            md5          = file_to_upload[0]
            filename     = file_to_upload[1]
            library_name = file_to_upload[2]
            if filename in filenames:
                logger.error("Duplicated file {}".format(filename))
                continue

        checksums = DkfzIngestFastqCommand.compute_checksums(list(filenames), threads=threads)

        passed_verif = list()
        for file_to_upload in files_to_upload:
            md5          = file_to_upload[0]
            filename     = file_to_upload[1]
            library_name = file_to_upload[2]
            if not filename in checksums.keys():
                logger.error("MD5 sum failed for {}".format(filename))
                continue
            if checksums[filename] != md5:
                logger.error("MD5 checksum in metafile & computed from {} are not equal".format(filename))
                continue
            passed_verif.append((md5, filename, library_name))

        return passed_verif
    
    @staticmethod
    def _build_commands(files_to_upload, landing_zone, threads=None, pattern="{library_name}/raw_data/{date}", date=None):
        if not date:
            date = str(datetime.date.today())
        if threads and threads > 1:
            threads = "-N {}".format(threads)
        else:
            threads = ""
        commands = []
        for file_to_upload in files_to_upload:
            md5          = file_to_upload[0]
            filename     = file_to_upload[1]
            library_name = file_to_upload[2]
            cmd = "imkdir -p i:{landing_zone}/{path}".format(
                landing_zone=landing_zone,
                path=pattern.format(library_name=library_name, date=date)
            )
            commands.append(cmd)
            target_path = "{landing_zone}/{path}/{filename}".format(
                landing_zone=landing_zone,
                path=pattern.format(library_name=library_name, date=date),
                libname=filename.name
            )
            cmd = "iput -aK {threads} {source} i:{target}".format(threads=threads, source=filename, target=target)
            commands.append(cmd)
            target_path = "{landing_zone}/{path}/{filename}.md5".format(
                landing_zone=landing_zone,
                path=pattern.format(library_name=library_name, date=date),
                libname=filename.name
            )
            cmd = "iput -aK {threads} {source}.md5sum i:{target}".format(threads=threads, source=filename, target=target)
            commands.append(cmd)
        return commands

    @staticmethod
    def _execute_commands(commands):
        for cmd in commands:
            try:
                cmd_str = " ".join(map(shlex.quote, cmd))
                logger.info("Executing %s", cmd_str)
                print(cmd)
                print(cmd_str)
                check_call(cmd)
            except SubprocessError as e:  # pragma: nocover
                logger.error("Problem executing command: %s", e)
                return 1
        return 0

    @staticmethod
    def compute_md5_checksum(filename, buffer_size=1048576):
        hash = None
        with open(filename, "rb") as f:
            hash = hashlib.md5()
            chunk = f.read(buffer_size)
            while chunk:
                hash.update(chunk)
                chunk = f.read(buffer_size)
        return hash.hexdigest()

    @staticmethod
    def _compute_md5_checksum_queue(checksums, queue, buffer_size=1048576):
        while True:
            filename = queue.get()
            checksums[filename] = DkfzIngestFastqCommand.compute_md5_checksum(
                filename, buffer_size=buffer_size
            )
            queue.task_done()

    @staticmethod
    def compute_checksums(filenames, threads=None):
        checksums = {}
        if threads and threads > 1:
            queue = Queue()
            for f in filenames:
                queue.put(f)
            for i in range(threads):
                worker = Thread(
                    target=DkfzIngestFastqCommand._compute_md5_checksum_queue,
                    args=(checksums, queue),
                )
                worker.setDaemon(True)
                worker.start()
            queue.join()
        else:
            for f in filenames:
                checksums[f] = DkfzIngestFastqCommand.compute_md5_checksum(f)
        return checksums

    @staticmethod
    def read_companion_md5(filename, ext=".md5sum"):
        lines = None
        with open(filename + ext, "r") as f:
            lines = f.readlines()
        if len(lines) != 1:
            raise ValueError(
                "Empty or multiple lines in checksum file {}".format(filename + ext)
            )
        m = DkfzIngestFastqCommand.md5_pattern.match(lines[0])
        if not m:
            raise ValueError(
                "MD5 checksum {} in file {} doesn't match expected pattern".format(
                    lines[0], filename + ext
                )
            )
        if m.group(2) != Path(filename).name:
            raise ValueError(
                "Checkum file {} doesn't report matching file (contents is {})".format(
                    filename + ext, lines[0]
                )
            )
        return m.group(1).lower()

    def build_commands(self, catalog):
        df = catalog[["folder_name", "library_name", "source_path"]]

        lz_irods_path = None
        if "/" in self.args.destination:
            lz_irods_path = self.args.destination
        else:
            lz_irods_path = landing_zones.get(
                sodar_url=self.args.sodar_url,
                sodar_api_token=self.args.sodar_api_token,
                landing_zone_uuid=self.args.destination,
            ).irods_path
            logger.info("Target iRods path: %s", lz_irods_path)
        if self.args.remote_dir_date:
            date = self.args.remote_dir_date
        else:
            date = str(datetime.date.today())
        threads = ""
        if self.args.num_parallel_transfers and self.args.num_parallel_transfers > 1:
            threads = "-N {}".format(threads)

        transfer_jobs = []
        for i in range(df.shape[0]):
            path = self.args.remote_dir_pattern.format(
                library_name=df.iloc[i, 0], date=date
            )
            source = df.iloc[i, 2]
            dest = "{lz}/{path}/{libname}".format(
                lz=lz_irods_path, path=path, libname=df.iloc[i, 1]
            )
            for ext in [("", ""), (".md5sum", ".md5")]:
                size = 0
                try:
                    size = os.path.getsize(source + ext[0])
                except OSError:  # pragma: nocover
                    size = 0
                path_src = source + ext[0]
                path_dest = dest + ext[1]
                transfer_jobs.append(
                    TransferJob(
                        path_src=path_src,
                        path_dest=path_dest,
                        command="irsync -a -K {} {} i:{}".format(
                            threads, path_src, path_dest
                        ),
                        bytes=size,
                    )
                )
        return transfer_jobs


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz ingest-fastq``."""
    return DkfzIngestFastqCommand.setup_argparse(parser)
