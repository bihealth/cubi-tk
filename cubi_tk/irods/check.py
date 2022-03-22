"""``cubi-tk irods check``: Check target iRods collection (all md5 files? metadata md5 consistent? enough replicas?)."""

import os
import sys
import argparse
import re
import typing
import uuid
from multiprocessing.pool import ThreadPool
from subprocess import check_output, SubprocessError
from retrying import retry

from logzero import logger
import tqdm


MIN_NUM_REPLICAS = 2
NUM_PARALLEL_TESTS = 8


class IrodsCheckCommand:
    """Implementation of irods check command."""

    command_name = "check"

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="irods_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-replicas",
            type=int,
            default=MIN_NUM_REPLICAS,
            help="Minimum number of replicas, defaults to %s" % MIN_NUM_REPLICAS,
        )

        parser.add_argument(
            "--num-parallel-tests",
            type=int,
            default=NUM_PARALLEL_TESTS,
            help="Number of parallel tests, defaults to %s" % NUM_PARALLEL_TESTS,
        )

        parser.add_argument("irods_path", help="Path to an iRods collection.")

    def get_files(self):
        """get files on iRods."""
        try:
            ils_out = check_output(["ils", "-r", self.args.irods_path]).decode(sys.stdout.encoding)
        except SubprocessError as e:  # pragma: nocover
            logger.error("Something went wrong: %s\nAre you logged in? try 'iinit'", e)
            raise
        files = dict(files=[], md5=[])
        base_path = None
        for line in ils_out.split("\n"):
            m = re.fullmatch(r"(\s+)?(C-\s)?(\S+)\s*", line)
            if m:
                g = m.groups()
                if not g[0]:
                    base_path = g[2][:-1]
                elif not g[1]:
                    ftype = "files" if g[2][-4:] != ".md5" else "md5"
                    files[ftype].append(os.path.join(base_path, g[2]))
        return files

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
        logger.info("  args: %s", self.args)

        self.run_tests(self.get_files())

        logger.info("All done")
        return None

    def run_tests(self, files):
        """Run tests in parallel."""
        num_files = len(files["files"])
        lst_files = "\n".join(files["files"][:19])
        logger.info("Checking %s files (first 20 shown):\n%s", num_files, lst_files)

        # counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=num_files, unit="files", unit_scale=False) as t:
            if self.args.num_parallel_tests == 0:
                for file in files["files"]:
                    check_file(file, files["md5"], self.args.num_replicas, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_tests)
                for file in files["files"]:
                    pool.apply_async(
                        check_file, args=(file, files["md5"], self.args.num_replicas, t)
                    )
                pool.close()
                pool.join()


@retry(wait_fixed=1000, stop_max_attempt_number=3)
def check_file(file, md5s, req_num_reps, t):
    """Perform checks for a single file."""

    # 1) md5 sum file exists?
    if file + ".md5" not in md5s:
        e_msg = f"No md5 sum file for: {file}"
        logger.error(e_msg)
        # raise FileNotFoundError(e_msg)

    # 2) enough replicas?
    try:
        isysmeta_out = check_output(["isysmeta", "-l", "ls", "{file}"]).decode(sys.stdout.encoding)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing isysmeta: %s (probably retrying)", e)
        raise
    meta_info = [
        {
            lst[0]: lst[1:]
            for lst in (entry.split(": ") for entry in repl.splitlines())
            if len(lst) > 0
        }
        for repl in isysmeta_out.split("----")
    ]
    if len(meta_info) < req_num_reps:
        e_msg = f"Not enough ({req_num_reps}) replicates for file: {file}"
        logger.error(e_msg)
        # raise FileNotFoundError(e_msg)

    # 3) checksum consistent with .md5 file?
    try:
        temp_file = f"./temp_{str(uuid.uuid4())}.md5"
        check_output(["irsync", "-aK", "i:{file}.md5", temp_file])
    except SubprocessError as e:  # pragma: nocover
        logger.error("Could not fetch file for md5 sum check: %s.md5: %s", temp_file, e)
        raise

    with open(temp_file, "r") as f:
        md5sum = re.match(r"\S+", f.read()).group(0)
    os.remove(temp_file)

    if not all(repl["data_checksum"][0] == md5sum for repl in meta_info):
        logger.error(
            "File checksum not consistent with md5 file...\n"
            "file: %s\n.md5-file checksum: %s\n"
            "metadata checksum: %s",
            file,
            md5sum,
            meta_info[0]["data_checksum"],
        )
        # raise ValueError(e_msg)

    # with counter.get_lock():
    #    counter.value += 1
    t.update()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk irods check``."""
    return IrodsCheckCommand.setup_argparse(parser)
