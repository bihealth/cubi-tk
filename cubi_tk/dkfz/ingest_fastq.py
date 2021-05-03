"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import datetime
import os
import re
import typing
import hashlib
from pathlib import Path
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError
from ctypes import c_ulonglong
import shlex

import tqdm
from logzero import logger

from .parser import DkfzMetaParser
from .DkfzMeta import DkfzMeta
from ..sodar.api import landing_zones
from ..snappy.itransfer_common import TransferJob, irsync_transfer
from ..common import sizeof_fmt

DEFAULT_NUM_TRANSFERS = 8

class DkfzIngestFastqCommand:
    """Implementation of dkfz ingest-fastq command for raw data."""

    command_name = "ingest-fastq"
    step_name = "raw_data"

    md5_pattern = re.compile("^([0-9a-f]{32}) +(.+)$", re.IGNORECASE)

    def __init__(self, args):
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="dkfz_cmd", default=cls.run, help=argparse.SUPPRESS
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
            dest="assay_type",
            choices = DkfzMetaParser.mappings["SEQUENCING_TYPE_to_Assays"].values(),
            default="exome",
            help="Assay type to upload to landing zone"
        )

        parser.add_argument(
            "--checks",
            dest="checks",
            action="store_true",
            help="Compute md5 checksums and verify them against metadata"
        )

        parser.add_argument(
            "--species",
            dest="species",
            default="human",
            help="Organism common name"
        )

        parser.add_argument(
            "--mapping",
            help=r"""
                File containing a table with the mapping between sample ids.
                The sample id stored in the DKFZ metafile is replaced by another id.
                The DKFZ sample id must be in column 'Sample Name', and the replacement id in column 'Sample Name CUBI'.
                When column 'Patient Name CUBI' is present in the mapping table, its contents is used for the 'Source Name' is the ISA-tab sample file.
                Additional columns are used to create 'Characteristics' columns for the sample material in the ISA-tab sample file.
            """
        )

        parser.add_argument("--dktk", default=False, action="store_true", help="Further heuristics to process DKTK Master-like samples")

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

        parser.add_argument("meta", nargs="+", help="DKFZ meta file(s)")
            

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
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk org-raw check")
        logger.info("  args: %s", self.args)

        catalog = self.prepare_files_catalog()
        jobs    = self.build_commands(catalog)
        
        logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), jobs)))

        logger.info("Planning to transfer the files as follows...")
        for job in jobs:
            logger.info("  %s => %s", job.path_src, job.path_dest)

        total_bytes = sum([job.bytes for job in jobs])
        logger.info(
            "Transferring %d files with a total size of %s", len(jobs), sizeof_fmt(total_bytes)
        )

        if not self.args.dry_run:
            counter = Value(c_ulonglong, 0)
            with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
                if self.args.num_parallel_transfers == 0:  # pragma: nocover
                    for job in jobs:
                        irsync_transfer(job, counter, t)
                else:
                    pool = ThreadPool(processes=self.args.num_parallel_transfers)
                    for job in jobs:
                        pool.apply_async(irsync_transfer, args=(job, counter, t))
                    pool.close()
                    pool.join()

        logger.info("All done")
        return None


    def prepare_files_catalog(self):
        dkfz_parser = DkfzMetaParser()
        all = None
        for filename in self.args.meta:
            meta = dkfz_parser.DkfzMeta(filename, species=self.args.species)
            if all is None:
                all = meta
            else:
                all.extend(meta)

        mapping = None
        if self.args.dktk:
            mapping = all.dktk()
        if self.args.mapping:
            mapping = pd.read_table(self.args.mapping)
        all.create_cubi_names(mapping=mapping)

        catalog = all.filename_mapping(assay_type=self.args.assay_type)

        if self.args.checks:
            checksums = DkfzIngestFastqCommand.compute_checksums(catalog["source_path"].tolist(), threads=self.args.num_parallel_transfers)
            if not DkfzIngestFastqCommand.verify_checksums(catalog, checksums):
                return None

        return catalog

        if self.args.dry_run:
            logger.info("Commands:\n{}\n".format("\n".join(commands)))
        else:
            DkfzIngestFastqCommand.execute_commands(commands)

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
            checksums[filename] = DkfzIngestFastqCommand.compute_md5_checksum(filename, buffer_size=buffer_size)
            queue.task_done()

    @staticmethod
    def compute_checksums(filenames, threads=None):
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
    def read_companion_md5(filename, ext=".md5sum"):
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
    def verify_checksums(catalog, checksums):
        try:
            catalog["companion_md5"] = [DkfzIngestFastqCommand.read_companion_md5(x) for x in catalog["source_path"].tolist()]
        except Exception as e:
            logger.error(str(e))
            return False
        catalog["computed_md5"] = [checksums[x] for x in catalog["source_path"]]
        j1 = catalog.columns.tolist().index("checksum")
        j2 = catalog.columns.tolist().index("computed_md5")
        j3 = catalog.columns.tolist().index("companion_md5")
        j0 = catalog.columns.tolist().index("source_path")
        j  = catalog.columns.tolist().index("folder_name")
        checksum_ok = True
        for i in range(catalog.shape[0]):
            if (catalog.iloc[i,j1] != catalog.iloc[i,j2]) or (catalog.iloc[i,j1] != catalog.iloc[i,j3]):
                checksum_ok = False
                logger.error("Checksums don't match for file {} (sample {})".format(catalog.iloc[i,j0], catalog.iloc[i,j]))
        return checksum_ok
    
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
            path   = self.args.remote_dir_pattern.format(library_name=df.iloc[i,0], date=date)
            source = df.iloc[i,2]
            dest   = "{lz}/{path}/{libname}".format(lz=lz_irods_path, path=path, libname=df.iloc[i,1])
            for ext in [("", ""), (".md5sum", ".md5")]:
                size = 0
                try:
                    size = os.path.getsize(source + ext[0])
                except OSError:  # pragma: nocover
                    size = 0
                path_src  = source + ext[0]
                path_dest = dest + ext[1]
                transfer_jobs.append(
                    TransferJob(
                        path_src=path_src,
                        path_dest=path_dest,
                        command="irsync -a -K {} {} i:{}".format(threads, path_src, path_dest),
                        bytes=size,
                    )
                )
        return transfer_jobs


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz ingest-fastq``."""
    return DkfzIngestFastqCommand.setup_argparse(parser)
