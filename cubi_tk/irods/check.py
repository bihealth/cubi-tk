"""``cubi-tk irods check``: Check target iRODS collection (all md5 files? metadata md5 consistent? enough replicas?)."""

import argparse
import getpass
import json
import os
import random
import re
import string
import sys
import tqdm
import typing

from irods.exception import CAT_NO_ROWS_FOUND
from irods.models import Collection, DataObject
from irods.query import SpecificQuery
from irods.session import iRODSSession
from logzero import logger
from multiprocessing.pool import ThreadPool

MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 4
MD5_RE = re.compile(r"[0-9a-fA-F]{32}")


class IrodsCheckCommand:
    """Implementation of iRDOS check command."""

    command_name = "check"

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

        #: iRODS sessions for parallel execution
        self.irods_sessions = []

        #: iRODS environment
        self.irods_env = None

        #: iRODS password
        self.irods_pass = None

    def __del__(self):
        # Ensure cleanup of iRODS sessions
        # NOTE: Possibly not necessary anymore with python-irodsclient v1.0.0?
        for irods in self.irods_sessions:
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
            "-n",
            "--num-parallel-tests",
            type=int,
            default=NUM_PARALLEL_TESTS,
            help="Number of parallel tests, defaults to %s" % NUM_PARALLEL_TESTS,
        )

        parser.add_argument("irods_path", help="Path to an iRODS collection.")

    @classmethod
    def get_irods_error(cls, e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es != "None" else e.__class__.__name__

    def get_irods_env(self):
        """Load iRODS environment from JSON file."""
        irods_env_path = os.path.join(os.path.expanduser("~"), ".irods", "irods_environment.json")
        try:
            with open(irods_env_path, "r") as f:
                self.irods_env = json.load(f)
        except Exception as e:
            logger.error("Failed to read ~/.irods/irods_environment.json: %s", e)
            raise

    def connect(self):
        """Connect to iRODS."""
        try:
            return iRODSSession(password=self.irods_pass, **self.irods_env)
        except Exception as e:
            logger.error("iRODS connection failed: %s", self.get_irods_error(e))
            raise

    def get_file_paths(self, irods: iRODSSession):
        """Get data objects recursively under the given iRODS path."""
        # NOTE: We don't use walk() as it is extremely slow and inefficient
        sql = (
            "SELECT DISTINCT ON (data_id) data_name, coll_name "
            "FROM r_data_main JOIN r_coll_main USING (coll_id) "
            "WHERE (coll_name = '{coll_path}' "
            "OR coll_name LIKE '{coll_path}/%')".format(coll_path=self.args.irods_path)
        )
        columns = [DataObject.name, Collection.name]
        query_alias = "cubi-tk_query_" + "".join(
            random.SystemRandom().choice(string.ascii_lowercase) for _ in range(16)
        )
        query = SpecificQuery(irods, sql, query_alias, columns)
        query.register()
        file_paths = dict(files=[], md5s=[])

        try:
            results = query.get_results()
            for row in results:
                path = row[Collection.name] + "/" + row[DataObject.name]
                k = "md5s" if path.endswith(".md5") else "files"
                file_paths[k].append(path)
        except CAT_NO_ROWS_FOUND:
            logger.info("No data objects found in iRODS path")
        except Exception as e:
            logger.error("Data object query failed: %s", self.get_irods_error(e))
        finally:
            query.remove()

        return file_paths

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

        # Get iRODS environment and user password
        self.get_irods_env()
        logger.info("iRODS environment: %s", self.irods_env)
        user_name = self.irods_env.get("irods_user_name")
        self.irods_pass = getpass.getpass(f"Password for user {user_name}: ", stream=sys.stderr)

        # Test connection
        irods = self.connect()
        try:
            irods.collections.get(self.args.irods_path)
            logger.info("iRODS connection initialized")
        except Exception as e:
            logger.error("Failed to retrieve iRODS path: %s", self.get_irods_error(e))
            raise

        # Get files and run checks
        self.run_checks(irods, self.get_file_paths(irods))
        logger.info("All done")

    def run_checks(self, irods: iRODSSession, file_paths: dict):
        """Run checks on files, in parallel if enabled."""
        num_files = len(file_paths["files"])
        lst_files = "\n".join(file_paths["files"][:19])
        logger.info(
            "Checking %s file%s%s:\n%s",
            num_files,
            "s" if num_files != 1 else "",
            " (first 20 shown)" if num_files > 20 else "",
            lst_files,
        )

        # counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=num_files, unit="files", unit_scale=False) as t:
            self.irods_sessions = [irods]

            if self.args.num_parallel_tests < 2:
                for path in file_paths["files"]:
                    check_file(
                        self.irods_sessions[0], path, file_paths["md5s"], self.args.req_num_reps, t
                    )
                return

            if num_files < self.args.num_parallel_tests:
                s_count = num_files
            else:
                s_count = self.args.num_parallel_tests
            self.irods_sessions += [self.connect() for _ in range(s_count - 1)]
            pool = ThreadPool(processes=self.args.num_parallel_tests)
            s_idx = 0
            for path in file_paths["files"]:
                pool.apply_async(
                    check_file,
                    args=(
                        self.irods_sessions[s_idx],
                        path,
                        file_paths["md5s"],
                        self.args.req_num_reps,
                        t,
                    ),
                )
                if s_idx == self.args.num_parallel_tests - 1:
                    s_idx = 0
                else:
                    s_idx += 1
            pool.close()
            pool.join()


def check_file(irods: iRODSSession, path: str, md5s: list, req_num_reps: int, t):
    """Perform checks for a single file."""
    data_obj = irods.data_objects.get(path)

    # 1) md5 sum file exists?
    if path + ".md5" not in md5s:
        e_msg = f"No md5 sum file for: {path}"
        logger.error(e_msg)

    # 2) enough replicas?
    if len(data_obj.replicas) < req_num_reps:
        e_msg = (
            f"Not enough replicas ({len(data_obj.replicas)} < " f"{req_num_reps}) for file: {path}"
        )
        logger.error(e_msg)

    # 3) checksums of all replicas consistent with .md5 file?
    md5_obj = irods.data_objects.open(path + ".md5", mode="r")
    file_sum = re.search(MD5_RE, md5_obj.read().decode("utf-8")).group(0)
    for replica in data_obj.replicas:
        if replica.checksum != file_sum:
            logger.error(
                "iRODS metadata checksum not consistent with MD5 file...\n"
                "File: %s\nMD5 file checksum: %s\n"
                "Metadata checksum: %s\nResource: %s",
                path,
                file_sum,
                replica.checksum,
                replica.resource_name,
            )

    # with counter.get_lock():
    #    counter.value += 1
    t.update()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk irods check``."""
    return IrodsCheckCommand.setup_argparse(parser)
