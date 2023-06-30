"""``cubi-tk irods check``: Check target iRODS collection (all md5 files? metadata md5 consistent? enough replicas?)."""

import argparse
from contextlib import contextmanager
import json
from multiprocessing.pool import ThreadPool
import os
import re
import typing

from irods.collection import iRODSCollection
from irods.data_object import iRODSDataObject
from irods.session import iRODSSession
from irods.models import Collection as CollectionModel
from irods.models import DataObject as DataObjectModel
from irods.column import Like


from logzero import logger
import tqdm

MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 4
NUM_DISPLAY_FILES = 20
HASH_SCHEMES = {
    "MD5": {"regex": re.compile(r"[0-9a-fA-F]{32}")},
    "SHA256": {"regex": re.compile(r"[0-9a-fA-F]{64}")},
}
DEFAULT_HASH_SCHEME = "MD5"


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
        parser.add_argument(
            "-s",
            "--hash-scheme",
            type=str,
            default=DEFAULT_HASH_SCHEME,
            help="Hash scheme used to verify checksums, defaults to %s" % DEFAULT_HASH_SCHEME,
        )
        parser.add_argument("irods_path", help="Path to an iRODS collection.")

    @classmethod
    def get_irods_error(cls, e: Exception):
        """Return logger friendly iRODS exception."""
        es = str(e)
        return es if es != "None" else e.__class__.__name__

    def get_data_objs(self, root_coll: iRODSCollection) -> typing.Dict[str, typing.Union[typing.Dict[str, iRODSDataObject], typing.List[iRODSDataObject]]]:
        """Get data objects recursively under the given iRODS path."""
        data_objs = dict(files=[], checksums=[])
        ignore_schemes = [k.lower() for k in HASH_SCHEMES if k != self.args.hash_scheme.upper()]
        irods_sess = root_coll.manager.sess

        query = irods_sess.query(
            DataObjectModel, CollectionModel
        ).filter(
            Like(CollectionModel.name, f"{root_coll.path}%")
        )

        for res in query:
            # If the 'res' dict is not split into Colllection&Object the resulting iRODSDataObject is not fully functional, likely because a name/path/... attribute is overwritten somewhere
            coll_res = {k: v for k,v in res.items() if k.icat_id >= 500}
            obj_res = {k: v for k,v in res.items() if k.icat_id < 500}
            coll = iRODSCollection(root_coll.manager, coll_res)
            obj = iRODSDataObject( irods_sess.data_objects, parent = coll, results=[obj_res])

            if obj.path.endswith("." + self.args.hash_scheme.lower()):
                data_objs["checksums"][obj.path] = obj
            elif obj.path.split(".")[-1] not in ignore_schemes:
                data_objs["files"].append(obj)

        return data_objs

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
            logger.error("iRODS environment not found in %s", self.irods_env_path)
            raise FileNotFoundError
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

        # Load iRODS environment
        with open(self.irods_env_path, "r", encoding="utf-8") as f:
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
            self.run_checks(data_objs)
            logger.info("All done")

    def run_checks(self, data_objs: typing.List[iRODSDataObject]):
        """Run checks on files, in parallel if enabled."""
        num_files = len(data_objs)
        dsp_files = data_objs
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
                    "File: %s\n%s file checksum: %s\n"
                    "Metadata checksum: %s\nResource: %s",
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
