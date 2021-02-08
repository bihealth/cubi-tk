"""``cubi-tk org-raw check``: checking of FASTQ files."""

import argparse
from ctypes import c_int, c_ulonglong
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
import subprocess
from pathlib import Path
import typing

import tqdm
from logzero import logger

from cubi_tk.common import sizeof_fmt


def _recreate_md5_for(path: Path):
    """Recreate .md5 file for.

    Return ``True`` if OK and ``False`` if not.
    """
    try:
        logger.debug("Calling `md5sum %s`", path)
        md5_out = subprocess.check_output(["md5sum", str(path)])
    except subprocess.SubprocessError as e:
        logger.info("Problem with MD5 computation: %s", e)
        return False

    md5_str = md5_out.decode("utf-8").strip().split()[0]
    logger.debug("MD5 sum of %s is %s", path.name, md5_str)

    try:
        with Path("%s.md5" % path).open("wt") as md5_file:
            logger.info("Writing MD5 sum to %s", md5_file.name)
            print("%s  %s" % (md5_str, path.name), file=md5_file)
    except IOError as e:
        logger.error("Problem writing MD5 sum: %e", e)
        return False
    else:
        return True


def _call(cmd: typing.List[str], cwd=None):
    logger.debug("Calling `%s`", " ".join(cmd))
    try:
        subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, cwd=cwd)
    except subprocess.CalledProcessError as e:
        tpl = (
            "md5sum --check call did not succeed: %s\n--- stdout BEGIN ---\n%s--- stdout END ---\n"
            "--- stderr BEGIN ---\n%s--- stderr END ---"
        )
        logger.error(
            tpl,
            e,
            "%s\n" % e.stdout.decode("utf-8") if e.stdout else "",
            "%s\n" % e.stderr.decode("utf-8") if e.stderr else "",
        )
        return False
    else:
        return True


def _check_md5(path: Path):
    """Check MD5 file at ``path``."""
    return _call(["md5sum", "--check", "%s.md5" % path.name], cwd=str(path.parent))


def _check_gz_integrity(path: Path):
    """Check GZip file integrity."""
    return _call(["gzip", "--test", str(path)])


def run_check(ok: Value, path: Path, args):
    """Run check without updating progress."""
    path_md5 = Path("%s.md5" % path)
    if not _check_gz_integrity(path):
        logger.error("GZip file integrity check failed: %s", path)
        with ok.get_lock():
            ok.value = False
    elif not path.exists():  # does not exist => error
        logger.error("Does not exist: %s", path)
        with ok.get_lock():
            ok.value = False
    elif path_md5.exists():
        logger.debug("MD5 file exists: %s", path_md5)
        if _check_md5(path):
            logger.debug("  => MD5 OK")
        else:
            logger.error("  => MD5 mismatch for %s", path)
            with ok.get_lock():
                ok.value = False
    elif args.missing_md5_error:  # => .md5 missing and is error => error
        logger.error("MD5 file does not exist for: %s", path)
        with ok.get_lock():
            ok.value = False
    elif args.compute_md5:  # .md5 missing and is not error => recreate
        recreated = _recreate_md5_for(path)
        if recreated:
            logger.info("Created MD5 file: %s", path_md5)
        elif args.create_md5_fail_error:
            logger.error("Could not create MD5 file for: %s", path)
            with ok.get_lock():
                ok.value = False
    else:
        logger.info("Not attempting to recreate %s", path_md5)


def _check(ok: Value, counter: Value, path: Path, args, t: tqdm.tqdm):
    run_check(ok, path, args)

    with counter.get_lock():
        counter.value += path.stat().st_size
        t.update(counter.value)


class CheckCommand:
    """Implementation of the ``check`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_options(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--num-threads", type=int, default=0, help="Number of parallel threads")
        parser.add_argument(
            "--no-gz-check",
            dest="check_gz",
            default=True,
            action="store_false",
            help="Deactivate check for gzip consistency (default is to perform check).",
        )
        parser.add_argument(
            "--no-md5-check",
            dest="check_md5",
            default=True,
            action="store_false",
            help="Deactivate comparison of MD5 sum if .md5 file exists (default is to perform check).",
        )
        parser.add_argument(
            "--no-compute-md5",
            dest="compute_md5",
            default=True,
            action="store_false",
            help="Deactivate computation of MD5 sum if missing (default is to compute MD5 sum).",
        )
        parser.add_argument(
            "--missing-md5-error",
            dest="missing_md5_error",
            default=False,
            action="store_true",
            help="Make missing .md5 files constitute an error. Default is to issue an log message only.",
        )
        parser.add_argument(
            "--create-md5-fail-no-error",
            dest="create_md5_fail_error",
            default=True,
            action="store_false",
            help="Make failure to create .md5 file not an error. Default is to make it an error.",
        )

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="org_raw_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        cls.setup_options(parser)

        parser.add_argument(
            "paths",
            metavar="FILE.fastq.gz",
            nargs="+",
            help="Path(s) to .fastq.gz files to perform the check for",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, _args):
        """Called for checking arguments, override to change behaviour."""
        return 0

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk org-raw check")
        logger.info("  args: %s", self.args)

        if self._run_loop():
            logger.info("All done.")
            return 0
        else:
            logger.error("Something went wrong.")
            return 1

    def _run_loop(self) -> int:
        """Run main loop."""
        total_bytes = sum([Path(p).stat().st_size for p in self.args.paths])
        logger.info(
            "Checking %d files with a total size of %s",
            len(self.args.paths),
            sizeof_fmt(total_bytes),
        )

        ok = Value(c_int, 1)
        counter = Value(c_ulonglong, 0)

        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_threads == 0:  # pragma: nocover
                for path in map(Path, self.args.paths):
                    _check(ok, counter, path, self.args, t)
            else:
                pool = ThreadPool(processes=self.args.num_threads)
                for path in map(Path, self.args.paths):
                    pool.apply_async(_check, args=(ok, counter, path, self.args, t))
                pool.close()
                pool.join()

        logger.info("All done")
        return ok.value


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return CheckCommand.setup_argparse(parser)
