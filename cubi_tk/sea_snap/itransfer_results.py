"""``cubi-tk sea-snap itransfer-ngs-mapping``: transfer ngs_mapping results into iRODS landing zone."""

import os
import sys
import argparse
import typing
import re
import pathlib
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError
from ctypes import c_ulonglong

import tqdm
from logzero import logger

from ..snappy.itransfer_common import SnappyItransferCommandBase, TransferJob
from ..common import check_irods_icommands, sizeof_fmt

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


class SeasnapItransferMappingResultsCommand(SnappyItransferCommandBase):
    """Implementation of sea-snap itransfer command for ngs_mapping results."""

    fix_md5_files = True
    command_name = "itransfer-results"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments"""

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

    def build_base_dir_glob_pattern(self, library_name: str) -> typing.Tuple[str, str]:
        pass

    def build_transfer_jobs(self, command_blocks, blueprint) -> typing.Tuple[TransferJob, ...]:
        """Build file transfer jobs."""
        transfer_jobs = []
        bp_mod_time = pathlib.Path(blueprint).stat().st_mtime

        if "/" in self.args.destination:
            lz_irods_path = self.args.destination
        else:
            from ..sodar.api import landing_zones

            lz_irods_path = landing_zones.get(
                sodar_url=self.args.sodar_url,
                sodar_api_token=self.args.sodar_api_token,
                landing_zone_uuid=self.args.destination,
            ).irods_path
            logger.info("Target iRods path: %s", lz_irods_path)

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
        return tuple(sorted(transfer_jobs))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sea-snap %s", self.command_name)
        logger.info("  args: %s", self.args)

        command_blocks = self.args.transfer_blueprint.read().split(os.linesep + os.linesep)
        transfer_jobs = self.build_transfer_jobs(command_blocks, self.args.transfer_blueprint.name)
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


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap itransfer-results``."""
    return SeasnapItransferMappingResultsCommand.setup_argparse(parser)


def irsync_transfer(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""
    if job.command:
        commands = job.command.split(os.linesep)
    else:
        msg = "Command attribute of TransferJob not set."
        logger.error(msg)
        raise ValueError(msg)

    for cmd in commands:
        logger.debug("Running command: %s", cmd)
        try:
            check_output(cmd, shell=True)
        except SubprocessError as e:  # pragma: nocover
            logger.error("Problem executing irsync: %e", e)
            raise

    with counter.get_lock():
        counter.value += job.bytes
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other
