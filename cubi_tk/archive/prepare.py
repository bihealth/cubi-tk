"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import attr
import os
import re
import sys
import time
import typing
import yaml

from logzero import logger

from ..common import compute_md5_checksum, execute_shell_commands
from . import common


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for prepare."""

    pass


class ArchivePrepareCommand(common.ArchiveCommandBase):
    """Implementation of archive prepare command."""

    command_name = "prepare"

    def __init__(self, config: Config):
        super().__init__(config)
        self.project_dir = None
        self.dest_dir = None

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument(
            "--rules",
            "-r",
            default=os.path.join(
                os.path.dirname(__file__), "..", "isa_tpl", "archive", "default_rules.yaml"
            ),
        )

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
        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the upload to sodar."""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        # extra_context = {}
        # for name in TEMPLATE.configuration:
        #     if getattr(self.config, "var_%s" % name, None) is not None:
        #         extra_context[name] = getattr(self.config, "var_%s" % name)

        logger.info("Starting cubi-tk archive prepare")
        logger.info("  args: %s", self.config)

        logger.info("Obtaining archive rules from {}".format(self.config.rules))
        with open(self.config.rules, "rt") as f:
            rules = yaml.safe_load(f)

        for (rule, patterns) in rules.items():
            compiled = []
            for pattern in patterns:
                compiled.append(re.compile(pattern))
            rules[rule] = compiled

        self.project_dir = os.path.realpath(self.config.project)
        self.dest_dir = os.path.realpath(self.config.destination)

        if os.path.exists(self.dest_dir):
            logger.error("Destination directory {} already exists".format(self.dest_dir))
            return 1

        self.start = time.time()
        self.nInode = 0

        self._read_dir_match(self.project_dir, rules)

        sys.stdout.write(" " * 80 + "\r")
        sys.stdout.flush()
        return 0

    def _read_dir_match(self, path, rules):
        self.nInode += 1
        if self.nInode % 1000 == 0:
            delta = int(time.time() - self.start)
            sys.stdout.write(
                "\rElapsed time: %02d:%02d:%02d, number of files processed: %d, rate: %.1f [files/sec]\r"
                % (
                    delta // 3600,
                    (delta % 3600) // 60,
                    delta % 60,
                    self.nInode,
                    self.nInode / delta if delta > 0 else 0,
                )
            )
            sys.stdout.flush()

        if not os.path.exists(path):
            logger.warning("File or directory {} cannot be read, not archived".format(path))
            return

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

        if os.path.isdir(path) and (
            not os.path.islink(path)
            or ArchivePrepareCommand._is_outside(os.path.realpath(path), self.project_dir)
        ):
            for child in os.listdir(path):
                self._read_dir_match(os.path.join(path, child), rules)
        else:
            self._archive(path)

    def _compress(self, path):
        if os.path.exists(path + ".tar.gz"):
            raise ValueError(
                "File or directory {} cannot be compressed, compressed file already exists".format(
                    path
                )
            )

        relative = os.path.relpath(path, start=self.project_dir)
        destination = os.path.join(self.dest_dir, relative)

        os.makedirs(os.path.dirname(destination), mode=488, exist_ok=True)
        command = [
            "tar",
            "-zcvf",
            destination + ".tar.gz",
            "--transform=s/^{}/{}/".format(os.path.basename(path), os.path.basename(destination)),
            "-C",
            os.path.dirname(path),
            os.path.basename(path),
        ]
        execute_shell_commands([command], verbose=self.config.verbose)

    def _squash(self, path):
        if os.path.isdir(path):
            raise ValueError("Path {} is a directory and cannot be squashed".format(path))

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
                relative_link = os.path.relpath(target, start=os.path.dirname(destination))
                os.symlink(relative_link, destination)
        else:
            os.symlink(os.path.realpath(path), destination)

    @staticmethod
    def _is_outside(path, directory):
        path = os.path.realpath(path)
        directory = os.path.realpath(directory)
        relative = os.path.relpath(path, start=directory)
        return relative.startswith("../")


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive find-file``."""
    return ArchivePrepareCommand.setup_argparse(parser)
