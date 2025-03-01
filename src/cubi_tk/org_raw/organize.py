"""``cubi-tk org-raw organize``: organization of FASTQ files"""

import argparse
from ctypes import c_int, c_ulonglong
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
from pathlib import Path
import re
import shutil
import typing

import attr
from loguru import logger
import tqdm

#: Default value for --src-regex.
from ..common import sizeof_fmt
from .check import CheckCommand, run_check

DEFAULT_SRC_REGEX = r"(.*/)?(?P<sample>.+)(?:-.+?)?\.f(?:ast)?q\.gz"

#: Default value for --dest-pattern
DEFAULT_DEST_PATTERN = r"{sample_name}/{file_name}"


@attr.s(auto_attribs=True, frozen=True)
class OrganizeJob:
    """Represent a file organization job."""

    #: Source location.
    src: Path
    #: Destination location.
    dest: Path


def _organize(ok: Value, counter: Value, job: OrganizeJob, args, t: tqdm.tqdm):
    logger.debug("{} {} to {}", "Moving" if args.move else "Copying", job.src, job.dest)
    if not job.dest.parent.exists():
        logger.debug("Creating directory {}", job.dest.parent)
        job.dest.parent.mkdir(parents=True)

    data_size = job.src.stat().st_size

    try:
        if args.dry_run:
            logger.debug("dry-run, not doing anything")
        elif args.move:
            shutil.move(str(job.src), str(job.dest))
        else:
            shutil.copy(str(job.src), str(job.dest))
    except OSError as e:
        logger.error("Problem with {} => {}: {}", job.src, job.dest, e)
        ok.value = False

    if not str(job.src).endswith(".md5") and Path("{}.md5".format(job.src)).exists():
        logger.debug(
            "  => also {} MD5 file {}.md5 {}.md5",
            "moving" if args.move else "copying",
            job.src,
            job.dest,
        )
        try:
            if args.dry_run:
                logger.debug("dry-run, not doing anything")
            elif args.move:
                shutil.move(str(job.src) + ".md5", str(job.dest) + ".md5")
            else:
                shutil.copy(str(job.src) + ".md5", str(job.dest) + ".md5")
        except OSError as e:
            logger.error("Problem with {} => {}: {}", job.src, job.dest, e)
            ok.value = False

    if args.dry_run:
        logger.debug("Not running check in dry-run mode.")
    else:
        run_check(ok, job.dest, args)

    with counter.get_lock():
        counter.value = data_size
        t.update(counter.value)


class OrganizeCommand:
    """Implementation of the ``organize`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="org_raw_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--dry-run",
            default=False,
            action="store_true",
            help="Dry-run, do not actually do anything",
        )
        parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Assume the answer to all prompts is 'yes'",
        )

        parser.add_argument(
            "--move",
            default=False,
            action="store_true",
            help="Move file(s) instead of copying, default is to copy.",
        )
        parser.add_argument(
            "--no-check",
            dest="check",
            default=True,
            action="store_false",
            help="Do not run 'raw-org check' on output (default is to run).",
        )

        parser.add_argument(
            "--src-regex",
            default=DEFAULT_SRC_REGEX,
            help="Regular expression for parsing file paths. Default: %s" % DEFAULT_SRC_REGEX,
        )
        parser.add_argument(
            "--dest-pattern",
            default=DEFAULT_DEST_PATTERN,
            help="Format expression for destination path generation. Default: %s"
            % DEFAULT_DEST_PATTERN,
        )

        CheckCommand.setup_options(parser)

        parser.add_argument("out_path", help="Path to output directory.")
        parser.add_argument(
            "input_paths", metavar="path.fastq.gz", help="Path to input files.", nargs="+"
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

        logger.info("Starting cubi-tk org-raw organize")
        logger.info("args: {}", self.args)

        jobs = []

        ok = True
        out_path = Path(self.args.out_path)
        for path in self.args.input_paths:
            m = re.match(self.args.src_regex, path)
            if not m:
                logger.error("Could not match with regex {}: {}", self.args.src_regex, path)
                ok = False
            else:
                logger.debug(
                    "Matched {} with regex {}: {}", path, self.args.src_regex, m.groupdict()
                )
                dest = out_path / self.args.dest_pattern.format(
                    sample_name=m.group("sample"), file_name=Path(path).name
                )
                if dest.exists():
                    logger.warning("Output path {} already exists, skipping.", dest)
                jobs.append(OrganizeJob(src=Path(path), dest=dest))

        if not ok:
            logger.error("Problem when processing input paths")
            return 1

        logger.info("Planning to {} the files as follows...", "move" if self.args.move else "copy")
        for job in jobs:
            logger.info(
                "  {} => {}{}",
                job.src,
                job.dest,
                " (+.md5)" if Path("{}.md5".format(job.src)).exists() else "",
            )
        if not self.args.yes and not input("Is this OK? [yN] ").lower().startswith("y"):
            logger.error("OK, breaking at your request")
            return 1

        return self._run_loop(jobs)

    def _run_loop(self, jobs: typing.Iterable[OrganizeJob]) -> int:
        """Run main loop."""
        total_bytes = sum([Path(p).stat().st_size for p in self.args.input_paths])
        logger.info(
            "Organizing {} files with a total size of {}",
            len(self.args.input_paths),
            sizeof_fmt(total_bytes),
        )

        ok = Value(c_int, 1)
        counter = Value(c_ulonglong, 0)

        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_threads == 0:  # pragma: nocover
                for job in jobs:
                    _organize(ok, counter, job, self.args, t)
            else:
                pool = ThreadPool(processes=self.args.num_threads)
                for job in jobs:
                    pool.apply_async(_organize, args=(ok, counter, job, self.args, t))
                pool.close()
                pool.join()

        logger.info("All done")
        return 1 - ok.value


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return OrganizeCommand.setup_argparse(parser)
