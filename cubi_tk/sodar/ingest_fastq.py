"""``cubi-tk sodar ingest-fastq``: add FASTQ files to SODAR"""

import os
import argparse
import datetime
from ctypes import c_ulonglong
import re
import typing
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError
import pathlib
import glob

from logzero import logger
import tqdm

from ..exceptions import MissingFileException
from ..sodar.api import landing_zones
from ..snappy.itransfer_common import SnappyItransferCommandBase, TransferJob, irsync_transfer

#: Default value for --src-regex.
from ..common import sizeof_fmt

DEFAULT_SRC_REGEX = (
    r"(.*/)?(?P<sample>.+?)"
    r"(?:_(?P<lane>L[0-9]+?))?"
    r"(?:_(?P<mate>R[0-9]+?))?"
    r"(?:_(?P<batch>[0-9]+?))?"
    r"\.f(?:ast)?q\.gz"
)

#: Default value for --dest-pattern
DEFAULT_DEST_PATTERN = r"{sample}/{date}/{filename}"

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


class SodarIngestFastq(SnappyItransferCommandBase):
    """Implementation of sodar ingest-fastq command."""

    fix_md5_files = True
    command_name = "ingest-fastq"

    def __init__(self, args):
        super().__init__(args)
        self.dest_pattern_fields = set(re.findall(r"(?<={).+?(?=})", self.args.remote_dir_pattern))

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR. Defaults to SODAR_API_TOKEN environment variable.",
        )

        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-parallel-transfers",
            type=int,
            default=DEFAULT_NUM_TRANSFERS,
            help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
        )
        parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Assume the answer to all prompts is 'yes'",
        )
        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help="Base path of project (contains 'ngs_mapping/' etc.), defaults to current path.",
        )
        parser.add_argument(
            "--remote-dir-date",
            default=datetime.date.today().strftime("%Y-%m-%d"),
            help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
        )
        parser.add_argument(
            "--src-regex",
            default=DEFAULT_SRC_REGEX,
            help=f"Regular expression to use for matching input fastq files, default: {DEFAULT_SRC_REGEX}",
        )
        parser.add_argument(
            "--remote-dir-pattern",
            default=DEFAULT_DEST_PATTERN,
            help=f"Pattern to use for constructing remote pattern, default: {DEFAULT_DEST_PATTERN}",
        )
        parser.add_argument(
            "--add-suffix",
            default="",
            help="Suffix to add to all file names (e.g. '-N1-DNA1-WES1').",
        )
        parser.add_argument(
            "-m",
            "--remote-dir-mapping",
            nargs=2,
            action="append",
            metavar=("MATCH", "REPL"),
            default=[],
            type=str,
            help="Substitutions applied to the filled remote dir paths. "
            "Can for example be used to modify sample names. "
            "Use pythons regex syntax of 're.sub' package. "
            "This argument can be used multiple times "
            "(i.e. '-m <regex1> <repl1> -m <regex2> <repl2>' ...).",
        )
        parser.add_argument(
            "--tmp",
            default="temp/",
            help="Folder to save files from WebDAV temporarily, if set as source.",
        )

        parser.add_argument("sources", help="paths to fastq folders", nargs="+")

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

    def build_base_dir_glob_pattern(self, library_name: str) -> typing.Tuple[str, str]:
        raise NotImplementedError(
            "build_base_dir_glob_pattern() not implemented in SodarIngestFastq!"
        )

    def download_webdav(self, sources):
        download_jobs = []
        folders = []
        for src in sources:
            if re.match("davs://", src):
                download_jobs.append(
                    TransferJob(path_src="i:" + src, path_dest=self.args.tmp, bytes=1)
                )
                tmp_folder = f"tmp_folder_{len(download_jobs)}"
                pathlib.Path(tmp_folder).mkdir(parents=True, exist_ok=True)
            else:
                folders.append(src)

        logger.info("Planning to download folders...")
        for job in download_jobs:
            logger.info("  %s => %s", job.path_src, job.path_dest)
        if not self.args.yes and not input("Is this OK? [yN] ").lower().startswith("y"):
            logger.error("OK, breaking at your request")
            return []

        counter = Value(c_ulonglong, 0)
        total_bytes = sum([job.bytes for job in download_jobs])
        with tqdm.tqdm(total=total_bytes) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in download_jobs:
                    download_folder(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in download_jobs:
                    pool.apply_async(download_folder, args=(job, counter, t))
                pool.close()
                pool.join()

        return folders

    def build_jobs(self, library_names=None) -> typing.Tuple[TransferJob, ...]:
        """Build file transfer jobs."""
        if library_names:
            logger.warning(
                "will ignore parameter 'library_names' = %s in build_jobs()", str(library_names)
            )

        if "/" in self.args.destination:
            lz_irods_path = self.args.destination
        else:
            lz_irods_path = landing_zones.get(
                sodar_url=self.args.sodar_url,
                sodar_api_token=self.args.sodar_api_token,
                landing_zone_uuid=self.args.destination,
            ).irods_path
            logger.info("Target iRods path: %s", lz_irods_path)

        transfer_jobs = []

        folders = self.download_webdav(self.args.sources)

        for folder in folders:
            logger.info("Searching for fastq files in folder: %s", folder)

            # assuming folder is local directory
            if not pathlib.Path(folder).is_dir():
                logger.error("Problem when processing input paths")
                raise MissingFileException("Missing folder %s" % folder)

            for path in glob.iglob(f"{folder}/**/*", recursive=True):
                real_path = os.path.realpath(path)

                if not os.path.isfile(real_path):
                    continue  # skip if did not resolve to file
                if real_path.endswith(".md5"):
                    continue  # skip, will be added automatically

                if not os.path.exists(real_path):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % real_path)
                if (
                    not os.path.exists(real_path + ".md5") and not self.fix_md5_files
                ):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % (real_path + ".md5"))

                m = re.match(self.args.src_regex, path)
                if m:
                    logger.debug(
                        "Matched %s with regex %s: %s", path, self.args.src_regex, m.groupdict()
                    )
                    match_wildcards = dict(
                        item
                        for item in m.groupdict(default="").items()
                        if item[0] in self.dest_pattern_fields
                    )
                    remote_file = pathlib.Path(lz_irods_path) / self.args.remote_dir_pattern.format(
                        filename=pathlib.Path(path).name + self.args.add_suffix,
                        date=self.args.remote_dir_date,
                        **match_wildcards,
                    )
                    remote_file = str(remote_file)
                    for m_pat, r_pat in self.args.remote_dir_mapping:
                        remote_file = re.sub(m_pat, r_pat, remote_file)

                    for ext in ("", ".md5"):
                        try:
                            size = os.path.getsize(real_path + ext)
                        except OSError:  # pragma: nocover
                            size = 0
                        transfer_jobs.append(
                            TransferJob(
                                path_src=real_path + ext, path_dest=remote_file + ext, bytes=size
                            )
                        )
        return tuple(sorted(transfer_jobs))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar %s", self.command_name)
        logger.info("  args: %s", self.args)

        jobs = self.build_jobs()
        logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), jobs)))

        if self.fix_md5_files:
            jobs = self._execute_md5_files_fix(jobs)

        logger.info("Planning to transfer the files as follows...")
        for job in jobs:
            logger.info("  %s => %s", job.path_src, job.path_dest)
        if not self.args.yes and not input("Is this OK? [yN] ").lower().startswith("y"):
            logger.error("OK, breaking at your request")
            return 1

        total_bytes = sum([job.bytes for job in jobs])
        logger.info(
            "Transferring %d files with a total size of %s", len(jobs), sizeof_fmt(total_bytes)
        )

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


def download_folder(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""

    irsync_argv = ["irsync", "-r", "-a", "-K", "i:%s" % job.path_src, job.path_dest]
    logger.debug("Transferring file: %s", " ".join(irsync_argv))
    try:
        check_output(irsync_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing irsync: %s", e)
        raise

    with counter.get_lock():
        counter.value += job.bytes
        t.update(counter.value)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return SodarIngestFastq.setup_argparse(parser)
