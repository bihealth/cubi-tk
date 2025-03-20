"""``cubi-tk irods check``: Check target iRODS collection (all md5 files? metadata md5 consistent? enough replicas?)."""

import argparse
import json
from multiprocessing.pool import ThreadPool
import os
import re
import typing

from irods.data_object import iRODSDataObject
from loguru import logger
import tqdm

from cubi_tk.parsers import print_args

from ..irods_common import DEFAULT_HASH_SCHEME, HASH_SCHEMES, iRODSRetrieveCollection

MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 4
NUM_DISPLAY_FILES = 20


class IrodsCheckCommand(iRODSRetrieveCollection):
    """Implementation of iRDOS check command."""

    command_name = "check"

    def __init__(self, args, hash_scheme=DEFAULT_HASH_SCHEME, ask=False, irods_env_path=None):
        """Constructor.

        :param args: argparse object with command line arguments.
        :type args: argparse.Namespace

        :param hash_scheme: iRODS hash scheme, default MD5.
        :type hash_scheme: str, optional

        :param ask: Confirm with user before certain actions.
        :type ask: bool, optional

        :param irods_env_path: Path to irods_environment.json
        :type irods_env_path: pathlib.Path, optional
        """
        super.__init__(hash_scheme, ask, irods_env_path)
        #: Command line arguments.
        self.args = args

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
        parser.add_argument(
            "-s",
            "--hash-scheme",
            type=str,
            default=DEFAULT_HASH_SCHEME,
            help="Hash scheme used to verify checksums, defaults to %s" % DEFAULT_HASH_SCHEME,
        )
        parser.add_argument("irods_path", help="Path to an iRODS collection.")

    def check_args(self, _args):
        # Check hash scheme
        if _args.hash_scheme.upper() not in HASH_SCHEMES:
            logger.error(
                'Invalid hash scheme "{}"; accepted values: {}'.format(
                    _args.hash_scheme.upper(), ", ".join(HASH_SCHEMES.keys())
                )
            )
            raise ValueError
        # Check environment file
        if not os.path.isfile(self.irods_env_path):
            logger.error("iRODS environment not found in {}", self.irods_env_path)
            raise FileNotFoundError
        return None

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(argparse.Namespace(**args)).execute()

    def execute(self):
        """Execute checks."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res
        logger.info("Starting cubi-tk irods {}", self.command_name)
        print_args(self.args)

        # Load iRODS environment
        with open(self.irods_env_path, "r", encoding="utf-8") as f:
            irods_env = json.load(f)
        logger.info("iRODS environment: {}", irods_env)

        # Connect to iRODS
        with self.session as irods_session:
            root_coll = irods_session.collections.get(self.args.irods_path)
            logger.info("1 iRODS connection initialized")
            # Get files and run checks
            logger.info("Querying for data objects")
            data_objs = self.get_data_objs(root_coll)
            self.run_checks(data_objs)
            logger.info("All done")

    def run_checks(self, data_objs: dict):
        """Run checks on files, in parallel if enabled."""
        num_files = len(data_objs["files"])
        dsp_files = data_objs["files"]
        if self.args.num_display_files > 0:
            dsp_files = dsp_files[: self.args.num_display_files]
        lst_files = "\n".join([f.path for f in dsp_files])
        logger.info(
            "Checking {} file{}{}:\n{}",
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
                    check_file(
                        obj,
                        data_objs["checksums"],
                        self.args.req_num_reps,
                        self.args.hash_scheme.upper(),
                        t,
                    )
                return

            pool = ThreadPool(processes=self.args.num_parallel_tests)
            s_idx = 0
            for obj in data_objs["files"]:
                pool.apply_async(
                    check_file,
                    args=(
                        obj,
                        data_objs["checksums"],
                        self.args.req_num_reps,
                        self.args.hash_scheme.upper(),
                        t,
                    ),
                )
                if s_idx == self.args.num_parallel_tests - 1:
                    s_idx = 0
                else:
                    s_idx += 1
            pool.close()
            pool.join()


def check_file(data_obj: iRODSDataObject, checksums: dict, req_num_reps: int, hash_scheme: str, t):
    """Perform checks for a single file."""
    chk_obj = checksums.get(data_obj.path + "." + hash_scheme.lower())

    # 1) Checksum file exists?
    if not chk_obj:
        e_msg = f"No checksum file for: {data_obj.path}"
        logger.error(e_msg)

    # 2) Checksums of all replicas consistent with checksum file?
    else:
        with chk_obj.open("r") as f:
            file_sum = re.search(
                HASH_SCHEMES[hash_scheme]["regex"], f.read().decode("utf-8")
            ).group(0)
        for replica in data_obj.replicas:
            if replica.checksum != file_sum:
                logger.error(
                    "iRODS metadata checksum not consistent with checksum file...\n"
                    "File: {}\n{} file checksum: {}\n"
                    "Metadata checksum: {}\nResource: {}",
                    data_obj.path,
                    hash_scheme,
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

    t.update()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk irods check``."""
    return IrodsCheckCommand.setup_argparse(parser)
