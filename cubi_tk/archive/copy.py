"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import attr
import os
import shutil
import subprocess
import typing

from logzero import logger

from . import common


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for prepare."""

    audit_file: str
    audit_result: str
    skip: typing.List[str]
    num_threads: int
    destination: str


class ArchiveCopyCommand(common.ArchiveCommandBase):
    """Implementation of archive copy command."""

    command_name = "copy"

    def __init__(self, config: Config):
        super().__init__(config)
        self.project_dir = None
        self.dest_dir = None
        self.skip = []

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument("--num-threads", type=int, default=4, help="Number of parallel threads")
        parser.add_argument(
            "--skip", type=str, nargs="*", help="Step to skip (hashdeep, rsync, audit)"
        )
        parser.add_argument("--audit-file", "-a", help="Hashdeep audit file")
        parser.add_argument(
            "--audit-result", "-r", help="Hashdeep audit result (of the copy check)"
        )
        parser.add_argument(
            "destination", help="Final destination directory for archive, must not exist"
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

        logger.info("Starting cubi-tk archive copy")
        logger.info("  args: %s", self.config)

        self.project_dir = os.path.realpath(self.config.project)
        self.dest_dir = os.path.realpath(self.config.destination)
        if self.config.skip:
            self.skip = self.config.skip

        if os.path.exists(self.dest_dir):
            logger.error("Destination directory {} already exists".format(self.dest_dir))
            return 1
        if not self.config.audit_file or not self.config.audit_result:
            logger.error("Missing path to either hashdeep output files")
            return 1

        removed = []
        removed = self._remove_relative_symlinks(self.project_dir, removed)
        logger.info("Set {} relative symlinks aside".format(len(removed)))

        status = 0
        hashdeep = ["hashdeep", "-j", str(self.config.num_threads), "-l", "-r"]
        try:
            if "hashdeep" not in self.skip:
                if not shutil.which("hashdeep"):
                    logger.error("hashdeep can't be found")
                    return 1
                # Hashdeep on the temporary destination
                logger.info("Preparing the hashdeep report to {}".format(self.config.audit_file))
                cmd = hashdeep + ["-o", "fl", "."]
                f = open(self.config.audit_file, "wt")
                p = subprocess.Popen(
                    cmd, cwd=self.project_dir, encoding="utf-8", stdout=f, stderr=subprocess.PIPE
                )
                p.communicate()
                if p.returncode != 0:
                    raise OSError(
                        "Command {} returned error code {}".format(" ".join(cmd), p.returncode)
                    )

            if "rsync" not in self.skip:
                # rsync -a without copy symlinks as symlinks, devices & special files
                logger.info("Copy files from {} to {}".format(self.project_dir, self.dest_dir))
                cmd = ["rsync", "-rptgo", "--copy-links", self.project_dir + "/", self.dest_dir]
                subprocess.run(cmd, check=True)

            if "audit" not in self.skip:
                if not shutil.which("hashdeep"):
                    logger.error("hashdeep can't be found")
                    return 1
                # Hashdeep audit
                logger.info("Audit of copy, results in {}".format(self.config.audit_result))
                cmd = hashdeep + ["-vvv", "-a", "-k", os.path.realpath(self.config.audit_file), "."]
                f = open(self.config.audit_result, "wt")
                p = subprocess.Popen(
                    cmd, cwd=self.dest_dir, encoding="utf-8", stdout=f, stderr=subprocess.PIPE
                )
                p.communicate()
                if p.returncode != 0:
                    logger.error("Audit failed, check {} for errors".format(self.config.audit_file))

            # Add relative symlinks to the copy
            self._restore_relative_symlinks(self.dest_dir, removed)
        except Exception as e:
            status = 1
            logger.error(e)
        finally:
            # Restore relative symlinks to the original temporary destination
            logger.info("Restoring relative symlinks")
            self._restore_relative_symlinks(self.project_dir, removed)

        return status

    def _remove_relative_symlinks(self, path, removed):
        if os.path.islink(path):
            if not os.readlink(path).startswith("/"):
                relative = os.path.relpath(path, start=self.project_dir)
                destination = os.readlink(path)
                os.remove(path)
                removed.append((relative, destination))
                return removed

        if os.path.isdir(path):
            for child in os.listdir(path):
                removed = self._remove_relative_symlinks(os.path.join(path, child), removed)
        return removed

    def _restore_relative_symlinks(self, root, removed, add_dangling=True):
        for (relative, destination) in removed:
            if add_dangling or os.path.exists(
                os.path.join(root, os.path.dirname(relative), destination)
            ):
                os.symlink(destination, os.path.join(root, relative))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive copy``."""
    return ArchiveCopyCommand.setup_argparse(parser)
