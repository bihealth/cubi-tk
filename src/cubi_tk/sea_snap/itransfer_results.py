"""``cubi-tk sea-snap itransfer-ngs-mapping``: transfer ngs_mapping results into iRODS landing zone."""

import argparse
import attr
from ctypes import c_ulonglong
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
import os
import pathlib
import re
from subprocess import STDOUT, SubprocessError, check_call, check_output
from retrying import retry
import sys

from loguru import logger
import tqdm

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import check_irods_icommands, sizeof_fmt
from ..snappy.itransfer_common import SnappyItransferCommandBase

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

    command: str | None = None

    def to_oneline(self):
        return ("{} -> {} ({}) [{}]".format(self.path_src, self.path_dest, self.bytes, self.command))


@retry(wait_fixed=1000, stop_max_attempt_number=5)
def _wait_until_ils_succeeds(path):
    check_output(["ils", path], stderr=STDOUT)


@retry(wait_fixed=1000, stop_max_attempt_number=5)
def irsync_transfer(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    mkdir_argv = ["imkdir", "-p", os.path.dirname(job.path_dest)]
    logger.debug("Creating directory when necessary: {}", " ".join(mkdir_argv))
    try:
        check_output(mkdir_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing imkdir: {} (probably retrying)", e)
        raise

    _wait_until_ils_succeeds(os.path.dirname(job.path_dest))

    irsync_argv = ["irsync", "-a", "-K", job.path_src, "i:{}".format(job.path_dest)]
    logger.debug("Transferring file: {}", " ".join(irsync_argv))
    try:
        check_output(irsync_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing irsync: {} (probably retrying)", e)
        raise

    with counter.get_lock():
        counter.value = job.bytes
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other


class SeasnapItransferMappingResultsCommand(SnappyItransferCommandBase):
    """Implementation of sea-snap itransfer command for ngs_mapping results."""

    fix_md5_files = True
    command_name = "itransfer-results"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments"""
        parser.add_argument(
            "--hidden-cmd", dest="sea_snap_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-parallel-transfers",
            type=int,
            default=DEFAULT_NUM_TRANSFERS,
            help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
        )
        parser.add_argument(
            "transfer_blueprint",
            type=argparse.FileType("rt"),
            help="Path to blueprint file to load. This file contains commands to sync "
            "files with iRODS. Blocks of commands separated by an empty line will be "
            "executed together in one thread.",
        )

        parser.add_argument("destination", help="UUID or iRods path of landing zone to move to.")

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        # Check presence of icommands when not testing.
        if "pytest" not in sys.modules:  # pragma: nocover
            check_irods_icommands(warn_only=False)

        return 0

    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:
        pass

    def build_transfer_jobs(self, command_blocks, blueprint) -> list[TransferJob]:
        """Build file transfer jobs."""
        transfer_jobs = []
        bp_mod_time = pathlib.Path(blueprint).stat().st_mtime

        if "/" in self.args.destination:
            lz_irods_path = self.args.destination
        else:

            sodar_api = SodarApi(self.args, with_dest=True, dest_string="destination")

            lz = sodar_api.get_landingzone_retrieve(self.args.destination)
            if lz is not None:
                lz_irods_path = lz.irods_path
                logger.info("Target iRods path: {}", lz_irods_path)
            else:
                logger.error("Target iRods path couldn't be retrieved")
                return transfer_jobs
        for cmd_block in (cb for cb in command_blocks if cb):
            sources = [
                word
                for word in re.split(r"[\n ]", cmd_block)
                if pathlib.Path(word).exists() and word != ""
            ]
            dests = re.findall(r"i:(__SODAR__/\S+)", cmd_block)  # noqa: W605
            for f_type, f in {"source": sources, "dest": dests}.items():
                if len(set(f)) != 1:
                    raise ValueError(
                        "Command block %s contains multiple or no %s files!\n"
                        "src: %s\ndest: %s"
                        % (cmd_block, f_type, ", ".join(sources), ", ".join(dests))
                    )
            source: str = sources[0]
            dest: str = dests[0]
            dest = dest.replace("__SODAR__", lz_irods_path)
            cmd_block = cmd_block.replace("__SODAR__", lz_irods_path)

            if pathlib.Path(source).suffix == ".md5":
                continue  # skip, will be added automatically

            if pathlib.Path(source).stat().st_mtime > bp_mod_time:
                raise ValueError(
                    "Blueprint %s was created before %s. "
                    "Please update the blueprint." % (blueprint, source)
                )

            for ext in ("", ".md5"):
                try:
                    size = os.path.getsize(source + ext)
                except OSError:  # pragma: nocover
                    size = 0
                transfer_jobs.append(
                    TransferJob(
                        path_src=source + ext,
                        path_dest=dest + ext,
                        command=cmd_block.replace(source, source + ext).replace(dest, dest + ext),
                        bytes=size,
                    )
                )
        return sorted(transfer_jobs, key=lambda x: x.to_oneline())

    def execute(self) -> int | None:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sea-snap {}", self.command_name)
        print_args(self.args)

        command_blocks = self.args.transfer_blueprint.read().split(os.linesep + os.linesep)
        transfer_jobs = self.build_transfer_jobs(command_blocks, self.args.transfer_blueprint.name)
        logger.debug("Transfer jobs:\n{}", "\n".join(x.to_oneline() for x in transfer_jobs))

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_fix(transfer_jobs)

        total_bytes = sum([job.bytes for job in transfer_jobs])
        logger.info(
            "Transferring {} files with a total size of {}",
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

    def _execute_md5_files_fix(self, transfer_jobs: list[TransferJob]) -> list[TransferJob]:
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
            "Computing MD5 sums for {} files of {} with up to {} processes",
            len(todo_jobs),
            sizeof_fmt(total_bytes),
            self.args.num_parallel_transfers,
        )
        logger.info("Missing MD5 files:\n{}", "\n".join(j.path_src for j in todo_jobs))
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
                path_src=j.path_src,
                path_dest=j.path_dest,
                bytes=os.path.getsize(j.path_src),
                command=j.command,
            )
            for j in todo_jobs
        ]
        return sorted(done_jobs + ok_jobs, key=lambda x: x.to_oneline())


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap itransfer-results``."""
    return SeasnapItransferMappingResultsCommand.setup_argparse(parser)


def compute_md5sum(job: TransferJob, counter: Value, t: tqdm.tqdm) -> None:
    """Compute MD5 sum with ``md5sum`` command."""
    dirname = os.path.dirname(job.path_src)
    filename = os.path.basename(job.path_src)[: -len(".md5")]
    path_md5 = job.path_src

    md5sum_argv = ["md5sum", filename]
    logger.debug("Computing MD5sum {} > {}", " ".join(md5sum_argv), filename + ".md5")
    try:
        with open(path_md5, "wt") as md5f:
            check_call(md5sum_argv, cwd=dirname, stdout=md5f)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing md5sum: {}", e)
        logger.info("Removing file after error: {}", path_md5)
        try:
            os.remove(path_md5)
        except OSError as e_rm:  # pragma: nocover
            logger.error("Could not remove file: {}", e_rm)
        raise e

    with counter.get_lock():
        counter.value = os.path.getsize(job.path_src[: -len(".md5")])
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other
