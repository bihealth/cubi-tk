"""Common code for ``cubi-sak snappy itransfer-*`` commands."""

import argparse
import datetime
import glob
import os
import typing
from ctypes import c_ulonglong
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError, check_call
import sys

import attr
from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from logzero import logger
import tqdm

from ..exceptions import MissingFileException
from ..common import check_irods_icommands, sizeof_fmt


#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


@attr.s(frozen=True, auto_attribs=True)
class TransferJob:
    """Encodes a transfer job from the local file system to the remote iRODS collection."""

    #: Source path.
    path_src: str

    #: Destination path.
    path_dest: str

    #: Number of bytes to transfer.
    bytes: int

    def to_oneline(self):
        return "%s -> %s (%s)" % (self.path_src, self.path_dest, self.bytes)


def irsync_transfer(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    mkdir_argv = ["imkdir", "-p", os.path.dirname(job.path_dest)]
    logger.debug("Creating directory when necessary: %s", " ".join(mkdir_argv))
    try:
        check_output(mkdir_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing imkdir: %e", e)
        raise

    irsync_argv = ["irsync", "-a", "-K", job.path_src, "i:%s" % job.path_dest]
    logger.debug("Transferring file: %s", " ".join(irsync_argv))
    try:
        check_output(irsync_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing irsync: %e", e)
        raise

    with counter.get_lock():
        counter.value += job.bytes
        t.update(counter.value)


def check_args(args):
    """Argument checks that can be checked at program startup but that cannot be sensibly checked with ``argparse``."""


def load_sheet_tsv(args):
    """Load sample sheet."""
    logger.info(
        "Loading %s sample sheet from %s.",
        args.tsv_shortcut,
        getattr(args.biomedsheet_tsv, "name", "stdin"),
    )
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % args.tsv_shortcut)
    return load_tsv(args.biomedsheet_tsv, naming_scheme=NAMING_ONLY_SECONDARY_ID)
    # sheet_class = getattr(shortcuts, "%sCaseSheet" % args.tsv_shortcut.title())
    # shortcut_sheet = sheet_class(sheet)


def all_ngs_library_names(sheet):
    """Yield all NGS library names from sheet"""
    for donor in sheet.bio_entities.values():
        for bio_sample in donor.bio_samples.values():
            for test_sample in bio_sample.test_samples.values():
                for library in test_sample.ngs_libraries.values():
                    yield library.name


class SnappyItransferCommandBase:
    """Base class for itransfer commands."""

    #: The command name.
    command_name: typing.Optional[str] = None
    #: The step folder name to create.
    step_name: typing.Optional[str] = None
    #: Whether or not to fix .md5 files on the fly.
    fix_md5_files: bool = False

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup common arguments for itransfer commands."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-parallel-transfers",
            type=int,
            default=DEFAULT_NUM_TRANSFERS,
            help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
        )
        parser.add_argument(
            "--tsv-shortcut",
            default="germline",
            choices=("germline", "cancer"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--start-batch",
            default=0,
            type=int,
            help="Batch to start the transfer at, defaults to 0.",
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
            "--remote-dir-pattern",
            default="{library_name}/%s/{date}" % cls.step_name,
            help="Pattern to use for constructing remote pattern",
        )

        parser.add_argument(
            "biomedsheet_tsv",
            type=argparse.FileType("rt"),
            help="Path to biomedsheets TSV file to load.",
        )
        parser.add_argument("irods_dest", help="path to iRODS collection to write to.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        # Check presence of icommands when not testing.
        if "pytest" not in sys.modules:  # pragma: nocover
            check_irods_icommands(warn_only=False)

        res = 0

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def build_base_dir_glob_pattern(
        self, library_name: str
    ) -> typing.Tuple[str, str]:  # pragma: nocover
        """Build base dir and glob pattern to append."""
        raise NotImplementedError("Abstract method called!")

    def build_jobs(self, library_names) -> typing.Tuple[TransferJob, ...]:
        """Build file transfer jobs."""
        transfer_jobs = []
        for library_name in library_names:
            base_dir, glob_pattern = self.build_base_dir_glob_pattern(library_name)
            glob_pattern = os.path.join(base_dir, glob_pattern)
            logger.debug("Glob pattern for library %s is %s", library_name, glob_pattern)
            for glob_result in glob.glob(glob_pattern, recursive=True):
                rel_result = os.path.relpath(glob_result, base_dir)
                real_result = os.path.realpath(glob_result)
                if real_result.endswith(".md5"):
                    continue  # skip, will be added automatically
                elif not os.path.isfile(real_result):
                    continue  # skip if did not resolve to file
                remote_dir = os.path.join(
                    self.args.irods_dest,
                    self.args.remote_dir_pattern.format(
                        library_name=library_name, date=self.args.remote_dir_date
                    ),
                )
                if not os.path.exists(real_result):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % real_result)
                if (
                    not os.path.exists(real_result + ".md5") and not self.fix_md5_files
                ):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % (real_result + ".md5"))
                for ext in ("", ".md5"):
                    try:
                        size = os.path.getsize(real_result + ext)
                    except OSError:  # pragma: nocover
                        size = 0
                    transfer_jobs.append(
                        TransferJob(
                            path_src=real_result + ext,
                            path_dest=os.path.join(remote_dir, rel_result + ext),
                            bytes=size,
                        )
                    )
        return tuple(sorted(transfer_jobs))

    def _execute_md5_files_fix(
        self, transfer_jobs: typing.Tuple[TransferJob, ...]
    ) -> typing.Tuple[TransferJob, ...]:
        """Create missing MD5 files."""
        ok_jobs = []
        todo_jobs = []
        for job in transfer_jobs:
            if not os.path.exists(job.path_src):
                todo_jobs.append(job)
            else:
                ok_jobs.append(job)

        total_bytes = sum([os.path.getsize(j.path_src[: -len(".md5")]) for j in todo_jobs])
        logger.info(
            "Computing MD5 sums for %s files of %s with up to %d processes",
            len(todo_jobs),
            sizeof_fmt(total_bytes),
            self.args.num_parallel_transfers,
        )
        logger.info("Missing MD5 files:\n%s", "\n".join(map(lambda j: j.path_src, todo_jobs)))
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in todo_jobs:
                    compute_md5sum(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in todo_jobs:
                    pool.apply_async(compute_md5sum, args=(job, counter, t))
                pool.close()
                pool.join()

        # Finally, determine file sizes after done.
        done_jobs = [
            TransferJob(
                path_src=j.path_src, path_dest=j.path_dest, bytes=os.path.getsize(j.path_src)
            )
            for j in todo_jobs
        ]

        return tuple(sorted(done_jobs + ok_jobs))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-sak snappy %s", self.command_name)
        logger.info("  args: %s", self.args)

        sheet = load_sheet_tsv(self.args)
        library_names = list(all_ngs_library_names(sheet))
        logger.info("Libraries in sheet:\n%s", "\n".join(sorted(library_names)))

        transfer_jobs = self.build_jobs(library_names)
        logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_fix(transfer_jobs)

        total_bytes = sum([job.bytes for job in transfer_jobs])
        logger.info(
            "Transferring %d files with a total size of %s",
            len(transfer_jobs),
            sizeof_fmt(total_bytes),
        )
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in transfer_jobs:
                    irsync_transfer(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in transfer_jobs:
                    pool.apply_async(irsync_transfer, args=(job, counter, t))
                pool.close()
                pool.join()

        logger.info("All done")
        return None


@attr.s(frozen=True, auto_attribs=True)
class FileWithSize:
    """Pair of path with size."""

    #: Path to file.
    path: str
    #: File size.
    bytes: int


def compute_md5sum(job: TransferJob, counter: Value, t: tqdm.tqdm) -> None:
    """Compute MD5 sum with ``md5sum`` command."""
    dirname = os.path.dirname(job.path_src)
    filename = os.path.basename(job.path_src)[: -len(".md5")]
    path_md5 = job.path_src

    md5sum_argv = ["md5sum", filename]
    logger.debug("Computing MD5sum %s > %s", " ".join(md5sum_argv), filename + ".md5")
    try:
        with open(path_md5, "wt") as md5f:
            check_call(md5sum_argv, cwd=dirname, stdout=md5f)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing md5sum: %e", e)
        logger.info("Removing file after error: %s", path_md5)
        try:
            os.remove(path_md5)
        except OSError as e_rm:  # pragma: nocover
            logger.error("Could not remove file: %e", e_rm)
        raise e

    with counter.get_lock():
        counter.value += os.path.getsize(job.path_src[: -len(".md5")])
        t.update(counter.value)
