"""``cubi-tk sea-snap itransfer-ngs-mapping``: transfer ngs_mapping results into iRODS landing zone."""

import argparse
import typing
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

from cubi_tk.irods_common import iRODSCommon, TransferJob
from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import check_irods_icommands, sizeof_fmt
from ..sodar_common import SodarIngestBase

class SeasnapItransferMappingResultsCommand(SodarIngestBase):
    """Implementation of sea-snap itransfer command for ngs_mapping results."""

    cubitk_section = "sea-snap"
    command_name = "itransfer-results"

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments"""
        parser.add_argument(
            "--hidden-cmd", dest="sea_snap_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "transfer_blueprint",
            type=argparse.FileType("rt"),
            help="Path to blueprint file to load. This file contains commands to sync "
            "files with iRODS. Blocks of commands separated by an empty line will be "
            "executed together in one thread.",
        )

    def build_jobs(self, hash_ending) -> list[TransferJob]:
        """Build file transfer jobs."""
        command_blocks = self.args.transfer_blueprint.read().split(os.linesep + os.linesep)
        blueprint = self.args.transfer_blueprint.name

        transfer_jobs = []
        bp_mod_time = pathlib.Path(blueprint).stat().st_mtime

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
            dest = dest.replace("__SODAR__", self.lz_irods_path)

            if pathlib.Path(source).suffix == hash_ending:
                continue  # skip, will be added automatically

            if pathlib.Path(source).stat().st_mtime > bp_mod_time:
                raise ValueError(
                    "Blueprint %s was created before %s. "
                    "Please update the blueprint." % (blueprint, source)
                )

            for ext in ("", hash_ending):
                transfer_jobs.append(
                    TransferJob(
                        path_local=source + ext,
                        path_remote=dest + ext,
                    )
                )
        return sorted(transfer_jobs, key=lambda x: x.path_local)

def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sea-snap itransfer-results``."""
    return SeasnapItransferMappingResultsCommand.setup_argparse(parser)
