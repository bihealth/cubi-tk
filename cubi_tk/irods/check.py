"""``cubi-tk irods check``: Check target iRODS collection (all md5 files? metadata md5 consistent? enough replicas?)."""

import argparse
import json
import os
import re
import tqdm
import typing

from contextlib import contextmanager
from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.session import iRODSSession
from logzero import logger
from multiprocessing.pool import ThreadPool

MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 4
NUM_DISPLAY_FILES = 20
MD5_RE = re.compile(r"[0-9a-fA-F]{32}")


class IrodsCheckCommand:
    """Implementation of iRDOS check command."""

    command_name = "check"

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

        #: Path to iRODS environment file
        self.irods_env_path = os.path.join(
            os.path.expanduser("~"), ".irods", "irods_environment.json"
        )

        #: iRODS environment
        self.irods_env = None

    def _init_irods(self):
        """Connect to iRODS."""
        try:
            return iRODSSession(irods_env_file=self.irods_env_path)
        except Exception as e:
            logger.error("iRODS connection failed: %s", self.get_irods_error(e))
            logger.error("Are you logged in? try 'iinit'")
            raise

    @contextmanager
    def _get_irods_sessions(self, count=NUM_PARALLEL_TESTS):
        if count < 1:
            count = 1
        irods_sessions = [self._init_irods() for _ in range(count)]
        try:
            yield irods_sessions
        finally:
            for irods in irods_sessions:
                irods.cleanup()

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="irods_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "-r",
            "--num-replicas",
            dest="req_num_reps",
            type=int,
            default=MIN_NUM_REPLICAS,
            help="Minimum number of replicas, defaults to %s" % MIN_NUM_REPLICAS,
        )

        parser.add_argument(
            "-p",
            "--num-parallel-tests",
            type=int,
            default=NUM_PARALLEL_TESTS,
            help="Number of parallel tests, defaults to %s" % NUM_PARALLEL_TESTS,
        )

        parser.add_argument(
            "-d",
            "--num-display-files",
            type=int,
            default=NUM_DISPLAY_FILES,
            help="Number of files listed when checking, defaults to %s" % NUM_DISPLAY_FILES,
        )

        parser.add_argument("irods_path", help="Path to an iRODS collection.")

    @classmethod
    def get_irods_error(cls, e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es != "None" else e.__class__.__name__

    def get_data_objs(self, root_coll: iRODSCollection):
        """Get data objects recursively under the given iRODS path."""
        data_objs = dict(files=[], md5s={})
        for res in root_coll.walk():
            for obj in res[2]:
                if obj.path.endswith(".md5"):
                    data_objs["md5s"][obj.path] = obj
                else:
                    data_objs["files"].append(obj)
        return data_objs

    def check_args(self, _args):
        return None

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self):
        """Execute checks."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res
        logger.info("Starting cubi-tk irods %s", self.command_name)
        logger.info("Args: %s", self.args)

        # Check for environment file
        if not os.path.isfile(self.irods_env_path):
            logger.error("iRODS environment not found in %s", self.irods_env_path)
            raise FileNotFoundError
        with open(self.irods_env_path, "r") as f:
            irods_env = json.load(f)
        logger.info("iRODS environment: %s", irods_env)

        # Connect to iRODS
        with self._get_irods_sessions(self.args.num_parallel_tests) as irods_sessions:
            try:
                root_coll = irods_sessions[0].collections.get(self.args.irods_path)
                logger.info(
                    "{} iRODS connection{} initialized".format(
                        len(irods_sessions), "s" if len(irods_sessions) != 1 else ""
                    )
                )
            except Exception as e:
                logger.error("Failed to retrieve iRODS path: %s", self.get_irods_error(e))
                raise

            # Get files and run checks
            logger.info("Querying for data objects")
            data_objs = self.get_data_objs(root_coll)
            self.run_checks(irods_sessions, data_objs)
            logger.info("All done")

    def run_checks(self, irods_sessions: list, data_objs: dict):
        """Run checks on files, in parallel if enabled."""
        num_files = len(data_objs["files"])
        dsp_files = data_objs["files"]
        if self.args.num_display_files > 0:
            dsp_files = dsp_files[: self.args.num_display_files]
        lst_files = "\n".join([f.path for f in dsp_files])
        logger.info(
            "Checking %s file%s%s:\n%s",
            num_files,
            "s" if num_files != 1 else "",
            " (first {} shown)".format(self.args.num_display_files)
            if self.args.num_display_files > 0 and num_files > self.args.num_display_files
            else "",
            lst_files,
        )

        # counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=num_files, unit="files", unit_scale=False) as t:
            if self.args.num_parallel_tests < 2:
                for obj in data_objs["files"]:
                    check_file(obj, data_objs["md5s"], self.args.req_num_reps, t)
                return

            if num_files < self.args.num_parallel_tests:
                s_count = num_files
            else:
                s_count = self.args.num_parallel_tests
            pool = ThreadPool(processes=self.args.num_parallel_tests)
            s_idx = 0
            for obj in data_objs["files"]:
                pool.apply_async(
                    check_file, args=(obj, data_objs["md5s"], self.args.req_num_reps, t)
                )
                if s_idx == self.args.num_parallel_tests - 1:
                    s_idx = 0
                else:
                    s_idx += 1
            pool.close()
            pool.join()


def check_file(data_obj: iRODSDataObject, md5s: dict, req_num_reps: int, t):
    """Perform checks for a single file."""
    md5_obj = md5s.get(data_obj.path + ".md5")

    # 1) MD5 sum file exists?
    if not md5_obj:
        e_msg = f"No md5 sum file for: {data_obj.path}"
        logger.error(e_msg)

    # 2) Checksums of all replicas consistent with .md5 file?
    else:
        with md5_obj.open("r") as f:
            file_sum = re.search(MD5_RE, f.read().decode("utf-8")).group(0)
        for replica in data_obj.replicas:
            if replica.checksum != file_sum:
                logger.error(
                    "iRODS metadata checksum not consistent with MD5 file...\n"
                    "File: %s\nMD5 file checksum: %s\n"
                    "Metadata checksum: %s\nResource: %s",
                    data_obj.path,
                    file_sum,
                    replica.checksum,
                    replica.resource_name,
                )

    # 3) Enough replicas?
    if len(data_obj.replicas) < req_num_reps:
        e_msg = (
            f"Not enough replicas ({len(data_obj.replicas)} < "
            f"{req_num_reps}) for file: {data_obj.path}"
        )
        logger.error(e_msg)

    # with counter.get_lock():
    #    counter.value += 1
    t.update()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk irods check``."""
    return IrodsCheckCommand.setup_argparse(parser)
