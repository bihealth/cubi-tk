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

        if os.path.exists(self.config.destination):
            logger.error("Destination directory {} already exists".format(self.config.destination))
            res = 1
        if not self.config.audit_file or not self.config.audit_result:
            logger.error("Missing path to either hashdeep output files")
            res = 1

        if not shutil.which("hashdeep") and (
            "hashdeep" not in self.config.skip and "audit" not in self.config.skip
        ):
            logger.error("hashdeep can't be found")
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Copies the contents of the input directory to the output path, following symlinks.
        The accuracy of the copy is verified by running hashdeep on the original files, and
        (in audit mode) on the copy.

        The copy module is meant to be executed after `cubi-tk archive prepare`. The prepare
        steps creates a temporary directory, with symlinks pointing to absolute paths of the
        files that must be copied. The copy is done using the `rsync` command, in a mode
        which follows symlinks.

        After the preparation step, relative symlinks pointing inside the project are retained.
        Those should not be copied by `rsync`, to avoid duplication of potentially large files.
        Therefore, the symlinks are deleted from the temporary directory before copy, and
        re-created after the copy is finished in both the original temporary directory and the
        final archive copy.
        """
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk archive copy")
        logger.info("  args: %s", self.config)

        self.project_dir = os.path.realpath(self.config.project)
        self.dest_dir = os.path.realpath(self.config.destination)

        # Find relative symlinks that point inside the project directory
        rel_symlinks = []
        rel_symlinks = self._find_relative_symlinks(self.project_dir, rel_symlinks)
        logger.info("Set {} relative symlinks aside".format(len(rel_symlinks)))
        # print("DEBUG- rel_symlinks = {}".format(rel_symlinks))

        status = 0
        hashdeep = ["hashdeep", "-j", str(self.config.num_threads), "-l", "-r"]
        try:
            if not self.config.skip or "hashdeep" not in self.config.skip:
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
                        "Command returned error code '{code}' : '{cmd}'".format(
                            code=p.returncode, cmd=" ".join(cmd)
                        )
                    )

            # Remove relative symlinks that point within the project to avoid file copy duplication
            self._remove_relative_symlinks(rel_symlinks)

            if not self.config.skip or "rsync" not in self.config.skip:
                # rsync -a without copy symlinks as symlinks, devices & special files
                logger.info("Copy files from {} to {}".format(self.project_dir, self.dest_dir))
                cmd = ["rsync", "-rptgo", "--copy-links", self.project_dir + "/", self.dest_dir]
                subprocess.run(cmd, check=True)

            # Add relative symlinks to the copy
            self._restore_relative_symlinks(self.dest_dir, rel_symlinks)

            if not self.config.skip or "audit" not in self.config.skip:
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
        except Exception as e:
            status = 1
            logger.error(e)
        finally:
            # Restore relative symlinks to the original temporary destination
            logger.info("Restoring relative symlinks")
            self._restore_relative_symlinks(self.project_dir, rel_symlinks)

        return status

    def _find_relative_symlinks(self, path, rel_symlinks):
        """Recursively traverse a directory (path) to find all relative symbolic links.
        The relative symlinks (symlink name & relative target) are stored in a list.
        """
        if os.path.islink(path):
            if not os.readlink(path).startswith("/"):
                relative = os.path.relpath(path, start=self.project_dir)
                destination = os.readlink(path)
                rel_symlinks.append((relative, destination))
                return rel_symlinks

        if os.path.isdir(path):
            for child in os.listdir(path):
                rel_symlinks = self._find_relative_symlinks(os.path.join(path, child), rel_symlinks)
        return rel_symlinks

    def _remove_relative_symlinks(self, rel_symlinks):
        """Remove relative symlinks from the original directory"""
        for (relative, _) in rel_symlinks:
            os.remove(os.path.join(self.project_dir, relative))

    def _restore_relative_symlinks(self, root, rel_symlinks, add_dangling=True):
        """Relative symlinks from list are added to the destination directory.

        root: str
            path to the root directory from which the symlinks are created
        rel_symlinks: List[Tuple[str, str]]
            list of symlinks.
            The first tuple element is the symlink path, relative to root.
            The second tuple element is the symlink target.
        add_dangling: bool
            controls if dangling symlinks must be created or not.
            It can happen that some relative symlinks point to missing file,
            for example if it belonged within a directory which has been squashed or
            ignored during the preparation step.
            The symlink should be there, even though the target file is not accessible
            in the archived copy.
        """
        for (relative, destination) in rel_symlinks:
            if add_dangling or os.path.exists(
                os.path.join(root, os.path.dirname(relative), destination)
            ):
                os.symlink(destination, os.path.join(root, relative))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive copy``."""
    return ArchiveCopyCommand.setup_argparse(parser)
