"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import attr
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import typing
import yaml

from cookiecutter.main import cookiecutter
from logzero import logger
from pathlib import Path

from . import common

import pdb

@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for find-file."""

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

        parser.add_argument("--rules", "-r", default=os.path.join(os.path.dirname(__file__), "default_rules.yaml"))

        parser.add_argument("destination", help="Destination directory (for symlinks and later archival)")


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

        self._read_dir_match(self.project_dir, rules)

        return 0

    def _read_dir_match(self, path, rules):
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

        if os.path.isdir(path):
            for child in os.listdir(path):
                self._read_dir_match(os.path.join(path, child), rules)
        else:
            self._archive(path)

    def _compress(self, path):
        if os.path.islink(path):
            target = os.path.realpath(os.readlink(path))
        else:
            target = path
        if os.path.exists(path + ".tar.gz"):
            logger.error("File or directory {} cannot be compressed".format(path))
            return
        fn_path = os.path.basename(path)
        dn_path = os.path.dirname(path)
        fn_target = os.path.basename(target)
        dn_target = os.path.dirname(target)
        reldir = os.path.relpath(dn_path, start=self.project_dir)
        dn_tar = os.path.join(self.dest_dir, reldir)
        fn_tar = os.path.join(dn_tar, fn_path + ".tar.gz")
        os.makedirs(dn_tar, mode=488, exist_ok=True)
        command = ["tar", "-zcvf", fn_tar, "--transform=s/^{}/{}/".format(fn_target, fn_path), "-C", dn_target, fn_target]
        self._execute_commands([command])

    def _squash(self, path):
        if os.path.islink(path):
            target = os.path.realpath(os.readlink(path))
        else:
            target = path
        if os.path.isdir(target):
            logger.error("Directory {} cannot be squashed".format(path))
            return
        relname = os.path.relpath(path, start=self.project_dir)
        squash_path = os.path.join(self.dest_dir, relname)
        os.makedirs(os.path.dirname(squash_path), mode=488, exist_ok=True)
        open(os.path.join(self.dest_dir, relname), "w").close()
        if not os.path.exists(path + ".md5"):
            md5 = ArchivePrepareCommand._compute_md5(target)
            with open(squash_path + ".md5", "w") as f:
                f.write(md5 + "  " + os.path.basename(path) + "\n")

    def _archive(self, path):
        relname = os.path.relpath(path, start=self.project_dir)
        archive_path = os.path.join(self.dest_dir, relname)
        os.makedirs(os.path.dirname(archive_path), mode=488, exist_ok=True)
        if os.path.islink(path):
            if os.readlink(path).startswith("/"):
                target = os.path.realpath(os.readlink(path))
            else:
                target = os.path.realpath(os.path.join(self.project_dir, os.path.dirname(path), os.readlink(path)))
            if ArchivePrepareCommand._is_outside(target, self.project_dir):
                os.symlink(target, archive_path)
            else:
                relpath = os.path.relpath(target, start=os.path.dirname(path))
                os.symlink(relpath, archive_path)
        else:
            os.symlink(path, archive_path)

    def _execute_commands(self, commands):
        previous = None
        for command in commands:
            if previous:
                current = subprocess.Popen(command, stdin=previous.stdout, stdout=subprocess.PIPE, encoding="utf-8")
                previous.stdout.close()
            else:
                current = subprocess.Popen(command, stdout=subprocess.PIPE, encoding="utf-8")
            previous = current
        return current.communicate()[0]

    @staticmethod
    def _is_outside(path, directory):
        relpath = os.path.relpath(path, directory)
        return (relpath.startswith("../"))

    @staticmethod
    def _compute_md5(filename, buffer_size=1048576):
        logger.info("Computing md5 hash for {}".format(filename))
        hash = None
        with open(filename, "rb") as f:
            hash = hashlib.md5()
            chunk = f.read(buffer_size)
            while chunk:
                hash.update(chunk)
                chunk = f.read(buffer_size)
        return hash.hexdigest()


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive find-file``."""
    return ArchivePrepareCommand.setup_argparse(parser)
