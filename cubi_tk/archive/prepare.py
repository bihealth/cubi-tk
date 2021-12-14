"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import attr
import datetime
import os
import re
import sys
import time
import typing
import yaml

from logzero import logger

from ..common import compute_md5_checksum, execute_shell_commands
from . import common
from .readme import create_readme
from .readme import add_readme_parameters


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for prepare."""

    rules: typing.Dict[
        str, typing.Any
    ]  # The regular expression string read from the yaml file in compiled into a re.Pattern
    skip: bool
    num_threads: int
    no_readme: bool
    destination: str


class ArchivePrepareCommand(common.ArchiveCommandBase):
    """Implementation of archive prepare command."""

    command_name = "prepare"

    def __init__(self, config: Config):
        super().__init__(config)
        self.project_dir = None
        self.dest_dir = None

        self.start = time.time()
        self.inode = 0

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument("--num-threads", type=int, default=4, help="Number of parallel threads")
        parser.add_argument(
            "--rules",
            "-r",
            default=os.path.join(
                os.path.dirname(__file__), "..", "isa_tpl", "archive", "default_rules.yaml"
            ),
        )
        parser.add_argument("--skip", "-s", action="store_true", help="Skip symlinks preparation")
        parser.add_argument("--no-readme", action="store_true", help="Skip README preparation")
        add_readme_parameters(parser)

        parser.add_argument(
            "destination", help="Destination directory (for symlinks and later archival)"
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        if not self.config.skip and os.path.exists(self.config.destination):
            logger.error("Destination directory {} already exists".format(self.config.destination))
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk archive prepare")
        logger.info("  args: %s", self.config)

        # Remove all symlinks to absolute paths
        self.project_dir = os.path.realpath(self.config.project)
        self.dest_dir = os.path.realpath(self.config.destination)

        os.makedirs(self.dest_dir, mode=488, exist_ok=False)

        if not self.config.no_readme:
            logger.info("Preparing README.md")
            create_readme(
                os.path.join(self.dest_dir, "README.md"), self.project_dir, config=self.config
            )

        if not self.config.skip:
            rules = self._get_rules(self.config.rules)

            # Recursively traverse the project and create archived files & links
            self._archive_path(self.project_dir, rules)

            sys.stdout.write(" " * 80 + "\r")
            sys.stdout.flush()

            # Run hashdeep on original project directory
            logger.info("Preparing the hashdeep report of {}".format(self.project_dir))
            res = common.run_hashdeep(
                directory=self.project_dir,
                out_file=os.path.join(
                    self.dest_dir, datetime.date.today().strftime("%Y-%m-%d_hashdeep_report.txt")
                ),
                num_threads=self.config.num_threads,
            )
            if res:
                logger.error("hashdeep command has failed with return code {}".format(res))
                return res

        return 0

    def _archive_path(self, path, rules):
        """Recursively archive files in the path, according to the rules"""
        self._progress()

        # Dangling link
        if not os.path.exists(path):
            logger.warning("File or directory cannot be read, not archived : '{}'".format(path))
            return

        # Check how the path should be processed by regular expression matching
        status = "archive"
        for (rule, patterns) in rules.items():
            for pattern in patterns:
                if pattern.match(path):
                    status = rule

        if status == "ignore":
            return
        if status == "compress":
            self._compress(path)
            return
        if status == "squash":
            self._squash(path)
            return
        assert status == "archive"

        # Archive files
        if not os.path.isdir(path):
            self._archive(path)
        else:
            # Process only true directories (not symlinks) or symlinks pointing outside of project
            if not os.path.islink(path) or self._is_outside(
                os.path.realpath(path), self.project_dir
            ):
                for child in os.listdir(path):
                    self._archive_path(os.path.join(path, child), rules)
            else:
                self._archive(path)

    def _progress(self):
        self.inode += 1
        if self.inode % 1000 == 0:
            delta = int(time.time() - self.start)
            sys.stdout.write(
                "\rElapsed time: %02d:%02d:%02d, number of files processed: %d, rate: %.1f [files/sec]\r"
                % (
                    delta // 3600,
                    (delta % 3600) // 60,
                    delta % 60,
                    self.inode,
                    self.inode / delta if delta > 0 else 0,
                )
            )
            sys.stdout.flush()

    def _compress(self, path):
        if os.path.exists(path + ".tar.gz"):
            raise ValueError(
                "File or directory cannot be compressed, compressed file already exists : '{}'".format(
                    path
                )
            )

        relative = os.path.relpath(path, start=self.project_dir)
        destination = os.path.join(self.dest_dir, relative)

        os.makedirs(os.path.dirname(destination), mode=488, exist_ok=True)
        cmd = [
            "tar",
            "-zcvf",
            destination + ".tar.gz",
            "--transform=s/^{}/{}/".format(os.path.basename(path), os.path.basename(destination)),
            "-C",
            os.path.dirname(path),
            os.path.basename(path),
        ]
        execute_shell_commands([cmd], verbose=self.config.verbose)

    def _squash(self, path):
        if os.path.isdir(path):
            raise ValueError("Path is a directory and cannot be squashed : '{}'".format(path))

        relative = os.path.relpath(path, start=self.project_dir)
        destination = os.path.join(self.dest_dir, relative)

        # Create empty placeholder
        os.makedirs(os.path.dirname(destination), mode=488, exist_ok=True)
        open(destination, "w").close()

        # Create checksum if missing
        if not os.path.exists(path + ".md5"):
            md5 = compute_md5_checksum(os.path.realpath(path), verbose=self.config.verbose)
            with open(destination + ".md5", "w") as f:
                f.write(md5 + "  " + os.path.basename(destination))

    def _archive(self, path):
        relative = os.path.relpath(path, start=self.project_dir)
        destination = os.path.join(self.dest_dir, relative)

        os.makedirs(os.path.dirname(destination), mode=488, exist_ok=True)
        if os.path.islink(path):
            target = os.path.realpath(path)
            relative_link = os.path.relpath(target, start=self.project_dir)
            if relative_link.startswith("../") or relative_link.startswith("/"):
                os.symlink(target, destination)
            else:
                os.symlink(os.readlink(path), destination)
        else:
            os.symlink(os.path.realpath(path), destination)

    @staticmethod
    def _get_rules(filename):
        logger.info("Obtaining archive rules from {}".format(filename))
        with open(filename, "rt") as f:
            rules = yaml.safe_load(f)

        for (rule, patterns) in rules.items():
            compiled = []
            for pattern in patterns:
                compiled.append(re.compile(pattern))
            rules[rule] = compiled

        return rules

    @staticmethod
    def _is_outside(path, directory):
        path = os.path.realpath(path)
        directory = os.path.realpath(directory)
        relative = os.path.relpath(path, start=directory)
        return relative.startswith("../")


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive prepare``."""
    return ArchivePrepareCommand.setup_argparse(parser)
