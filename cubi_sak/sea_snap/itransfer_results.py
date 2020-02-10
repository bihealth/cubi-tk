"""``cubi-sak sea-snap itransfer-ngs-mapping``: transfer ngs_mapping results into iRODS landing zone."""

import os
import sys
import argparse
import typing
import re
from pathlib import Path
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError, check_call
from ctypes import c_ulonglong

import attr
import tqdm
from logzero import logger

from ..snappy.itransfer_common import SnappyItransferCommandBase
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

    #: Commands for transfer.
    command: str

    #: Number of bytes to transfer.
    bytes: int

    def to_oneline(self):
        return "%s -> %s (%s)" % (self.path_src, self.path_dest, self.bytes)


class SeasnapItransferMappingResultsCommand(SnappyItransferCommandBase):
    """Implementation of sea-snap itransfer command for ngs_mapping results."""

    fix_md5_files = True
    command_name = "itransfer-mapping-results"
    step_name = "ngs_mapping"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
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
        parser.add_argument("irods_dest", help="path to iRODS collection to write to.")

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        # Check presence of icommands when not testing.
        if "pytest" not in sys.modules:  # pragma: nocover
            check_irods_icommands(warn_only=False)

        return 0

    def build_transfer_jobs(self, command_blocks, blueprint) -> typing.Tuple[TransferJob, ...]:
        """Build file transfer jobs."""
        transfer_jobs = []
        bp_mod_time = Path(blueprint).stat().st_mtime

        for cmd_block in (cb for cb in command_blocks if cb):
            sources = [word for word in re.split("\n| ", cmd_block) if Path(word).exists()]
            dests = re.findall("i:(__SODAR__/\S+)", cmd_block)  # noqa: W605
            for f_type, f in {"source": sources, "dest": dests}.items():
                if len(set(f)) != 1:
                    raise ValueError(
                        "Command block %s contains multiple or no %s files!" % (cmd_block, f_type)
                    )
            source: str = sources[0]
            dest: str = dests[0]
            dest = dest.replace("__SODAR__", self.args.irods_dest)
            cmd_block = cmd_block.replace("__SODAR__", self.args.irods_dest)

            if Path(source).suffix == ".md5":
                continue  # skip, will be added automatically

            if Path(source).stat().st_mtime > bp_mod_time:
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
        return tuple(sorted(transfer_jobs))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-sak sea-snap %s", self.command_name)
        logger.info("  args: %s", self.args)

        command_blocks = self.args.transfer_blueprint.read().split(os.linesep + os.linesep)
        transfer_jobs = self.build_transfer_jobs(command_blocks, self.args.transfer_blueprint.name)
        logger.debug("Transfer jobs:\n%s", "\n".join(map(lambda x: x.to_oneline(), transfer_jobs)))

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_transfer_fix(transfer_jobs)

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

    def _execute_md5_files_transfer_fix(
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
                path_src=j.path_src,
                path_dest=j.path_dest,
                bytes=os.path.getsize(j.path_src),
                command=j.command,
            )
            for j in todo_jobs
        ]

        return tuple(sorted(done_jobs + ok_jobs))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak sea-snap itransfer-results``."""
    return SeasnapItransferMappingResultsCommand.setup_argparse(parser)


def irsync_transfer(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    commands = job.command.split(os.linesep)

    for cmd in commands:
        cmd_argv = re.split(" +", cmd)
        logger.debug("Running command: %s", " ".join(cmd_argv))
        try:
            check_output(cmd_argv)
        except SubprocessError as e:  # pragma: nocover
            logger.error("Problem executing irsync: %e", e)
            raise

    with counter.get_lock():
        counter.value += job.bytes
        t.update(counter.value)


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
