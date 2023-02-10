"""``cubi-tk archive prepare``: Prepare a project for archival"""

import argparse
import datetime
import os
import re
import shutil
import subprocess
import tempfile
import typing

import attr
from logzero import logger

from . import common, readme
from ..common import execute_shell_commands
from ..exceptions import InvalidReadmeException, MissingFileException


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for prepare."""

    skip: typing.List[str]
    num_threads: int
    check_work_dest: bool
    read_only: bool
    destination: str


HASHDEEP_REPORT_PATTERN = re.compile(
    "^(([0-9]{4})-([0-9]{2})-([0-9]{2}))_hashdeep_(report|audit).txt$"
)


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
        parser.add_argument(
            "--keep-workdir-hashdeep",
            default=False,
            action="store_true",
            help="Save hashdeep report & audit of the temporary destination",
        )
        parser.add_argument(
            "--read-only",
            default=False,
            action="store_true",
            help="Change destination files to read-only",
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

        tmpdir = tempfile.TemporaryDirectory()

        status = 0
        try:
            if not readme.is_readme_valid(os.path.join(self.project_dir, "README.md")):
                raise InvalidReadmeException("README.md file missing or invalid")
            if not self.config.skip or "check_work" not in self.config.skip:
                work_report = os.path.join(
                    tmpdir.name, datetime.date.today().strftime("%Y-%m-%d_workdir_report.txt")
                )
                logger.info(
                    "Preparing hasheep report of {} to {}".format(self.project_dir, work_report)
                )
                self._hashdeep_report(self.project_dir, work_report)

            if not self.config.skip or "rsync" not in self.config.skip:
                # Remove relative symlinks that point within the project to avoid file copy duplication
                self._remove_relative_symlinks(rel_symlinks)

                self._rsync(self.project_dir, self.dest_dir)

                # Add relative symlinks to the copy
                self._restore_relative_symlinks(self.dest_dir, rel_symlinks)

            if not self.config.skip or "check_work" not in self.config.skip:
                work_audit = os.path.join(
                    tmpdir.name, datetime.date.today().strftime("%Y-%m-%d_workdir_audit.txt")
                )
                self._hashdeep_audit(self.dest_dir, work_report, work_audit)

            if not self.config.skip or "audit" not in self.config.skip:
                report = self._find_hashdeep_report(self.project_dir)
                audit = os.path.join(
                    tmpdir.name, datetime.date.today().strftime("%Y-%m-%d_hashdeep_audit.txt")
                )
                self._hashdeep_audit(self.dest_dir, report, audit)
                shutil.move(audit, os.path.join(self.dest_dir, os.path.basename(audit)))

            if res != 0 or self.config.keep_workdir_hashdeep:
                shutil.move(work_report, os.path.join(self.dest_dir, os.path.basename(work_report)))
                shutil.move(work_audit, os.path.join(self.dest_dir, os.path.basename(work_audit)))

            if readme.is_readme_valid(os.path.join(self.dest_dir, "README.md")):
                open(os.path.join(self.dest_dir, "archive_copy_complete"), "w").close()
                if self.config.read_only:
                    execute_shell_commands([["chmod", "-R", "ogu-w", self.dest_dir]])
            else:
                raise MissingFileException("Missing or illegal README.md file")
        except Exception as e:
            status = 1
            logger.error(e)
        finally:
            # Restore relative symlinks to the original temporary destination
            logger.info("Restoring relative symlinks")
            self._restore_relative_symlinks(self.project_dir, rel_symlinks)

        return status

    def _rsync(self, origin, destination):
        # rsync -a without copy symlinks as symlinks, devices & special files
        logger.info("Copy files from {} to {}".format(origin, destination))
        cmd = ["rsync", "-rptgo", "--copy-links", origin + "/", destination]
        subprocess.run(cmd, check=True)

    def _find_hashdeep_report(self, directory):
        ref_files = list(
            filter(
                lambda x: HASHDEEP_REPORT_PATTERN.match(x)
                and HASHDEEP_REPORT_PATTERN.match(x).group(5) == "report",
                os.listdir(self.project_dir),
            )
        )
        if not ref_files:
            raise MissingFileException("Cannot find hashdeep report to perform audit")
        ref_files.sort(reverse=True)
        return ref_files[0]

    def _hashdeep_report(self, directory, report):
        """Runs hashdeep in report mode, raising an exception in case of error"""
        res = common.run_hashdeep(
            directory=directory, out_file=report, num_threads=self.config.num_threads
        )
        if res != 0:
            raise subprocess.SubprocessError("Hashdeep report failed")

    def _hashdeep_audit(self, directory, report, audit):
        """Runs hashdeep in audit mode. Missing or added files are ignored"""
        logger.info("Audit of {} from {}".format(directory, report))
        res = common.run_hashdeep(
            directory=directory,
            out_file=audit,
            num_threads=self.config.num_threads,
            ref_file=report,
        )
        if res == 0:
            logger.info("Audit passed without errors")
        elif res == 1 or res == 2:
            logger.warning(
                "Audit found missing or changed files, check {} for errors".format(
                    os.path.basename(audit)
                )
            )
        else:
            raise subprocess.SubprocessError("Audit failed, check {} for errors".format(audit))

    def _find_relative_symlinks(self, path, rel_symlinks):
        """Recursively traverse a directory (path) to find all relative symbolic links.
        The relative symlinks (symlink name & relative target) are stored in a list.
        """
        if os.path.islink(path):
            if not os.path.isabs(os.readlink(path)):
                relative = os.path.relpath(path, start=self.project_dir)
                target = os.readlink(path)
                rel_symlinks.append((relative, target))
                return rel_symlinks

        if os.path.isdir(path):
            for child in os.listdir(path):
                rel_symlinks = self._find_relative_symlinks(os.path.join(path, child), rel_symlinks)
        return rel_symlinks

    def _remove_relative_symlinks(self, rel_symlinks):
        """Remove relative symlinks from the original directory"""
        for relative, _ in rel_symlinks:
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
        for relative, target in rel_symlinks:
            symlink_path = os.path.join(root, relative)
            if os.path.exists(symlink_path) or os.path.islink(symlink_path):
                continue
            symlink_dir = os.path.dirname(symlink_path)
            if add_dangling or os.path.exists(os.path.join(symlink_dir, target)):
                os.makedirs(symlink_dir, mode=488, exist_ok=True)  # 488 is 750 in octal
                os.symlink(target, symlink_path)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive copy``."""
    return ArchiveCopyCommand.setup_argparse(parser)
