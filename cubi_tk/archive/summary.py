"""``cubi-tk archive raw-data``: Checks for the presence of raw data in the project directory"""

import argparse
import attr
import os
import re
import sys
import time
import typing
import yaml

from pathlib import Path
from logzero import logger

from . import common
from .common import traverse_project_files


@attr.s(frozen=True, auto_attribs=True)
class Config(common.Config):
    """Configuration for find-file."""

    table: str


@attr.s(frozen=True, auto_attribs=True)
class StatClass:
    """Special cases for reporting files"""

    name: str
    min_size: int
    pattern: re.Pattern


class ArchiveSummaryCommand(common.ArchiveCommandBase):
    """Implementation of archive summary command."""

    command_name = "find-file"

    def __init__(self, config: Config):
        super().__init__(config)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        super().setup_argparse(parser)

        parser.add_argument(
            "--classes",
            default=os.path.join(
                os.path.dirname(__file__), "..", "isa_tpl", "archive", "classes.yaml"
            ),
            help="Location of the file describing files of interest",
        )
        parser.add_argument("table", help="Location of the summary output table")

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
        """Traverse all project files for summary"""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk archive summary")
        logger.info("  args: %s", self.config)

        self.classes = self._load_classes(self.config.classes)

        f = open(self.config.table, "wt") if self.config.table else sys.stdout

        resolved = Path(self.config.project)
        title = "# Files in {}".format(self.config.project)
        if self.config.project != str(resolved):
            title += " (resolved to {})".format(str(resolved))
        print(title, file=f)
        print(
            "\t".join(
                ["Class", "FileName", "Target", "ResolvedName", "Size", "Dangling", "Outside"]
            ),
            file=f,
        )

        stats = {
            "nFile": 0,
            "size": 0,
            "nLink": 0,
            "nDangling": 0,
            "nInaccessible": 0,
            "nOutside": 0,
            "size_outside": 0,
            "classes": {},
        }
        for theClass in self.classes:
            stats["classes"][theClass.name] = {
                "nFile": 0,
                "size": 0,
                "nLost": 0,
                "nOutside": 0,
                "size_outside": 0,
            }

        self.start = time.time()
        for file_attr in traverse_project_files(self.config.project):
            self._aggregate(file_attr, stats, f)

        # Clear the progress line
        if self.config.table:
            sys.stdout.write(" " * 80 + "\r")
            sys.stdout.flush()
        f.close()

        logger.info("Number of files in {}: {}".format(self.config.project, stats["nFile"]))
        logger.info(
            "Number of links: {} ({} dangling, {} inaccessible (permissions), {} outside of project directory)".format(
                stats["nLink"], stats["nDangling"], stats["nInaccessible"], stats["nOutside"]
            )
        )
        logger.info(
            "Total size: {} ({} in files outside of the directory)".format(
                stats["size"], stats["size_outside"]
            )
        )
        for (name, theStat) in stats["classes"].items():
            logger.info(
                "Number of {} files: {} (total size: {})".format(
                    name, theStat["nFile"], theStat["size"]
                )
            )
            logger.info(
                "Number of files outside the projects directory: {} (total size: {})".format(
                    theStat["nOutside"], theStat["size_outside"]
                )
            )
            logger.info(
                "Number of files lost (dangling or inaccessible): {}".format(theStat["nLost"])
            )

        return 0

    @staticmethod
    def _load_classes(f=None):
        if not f:
            f = sys.stdout
        if isinstance(f, str):
            f = open(f, "rt")
        classes = []
        for (name, params) in yaml.safe_load(f).items():
            classes.append(
                StatClass(
                    name=name,
                    min_size=int(params["min_size"]),
                    pattern=re.compile(params["pattern"]),
                )
            )
        return classes

    def _aggregate(self, file_attr, stats, f):
        save = []

        stats["nFile"] += 1
        stats["size"] += file_attr.size

        # symlinks
        if file_attr.target:
            stats["nLink"] += 1
            if file_attr.dangling:
                stats["nDangling"] += 1
                save.append("dangling")
            if file_attr.dangling is None:
                stats["nInaccessible"] += 1
                save.append("inaccessible")
            if file_attr.outside:
                stats["nOutside"] += 1
                stats["size_outside"] += file_attr.size
                save.append("outside")

        # File classes
        for theClass in self.classes:
            if (
                not theClass.pattern.match(file_attr.relative_path)
                or file_attr.size < theClass.min_size
            ):
                continue
            save.append(theClass.name)
            stats["classes"][theClass.name]["nFile"] += 1
            if file_attr.target:
                if file_attr.dangling is None or file_attr.dangling:
                    stats["classes"][theClass.name]["nLost"] += 1
                else:
                    if file_attr.outside:
                        stats["classes"][theClass.name]["nOutside"] += 1
                        stats["classes"][theClass.name]["size_outside"] += file_attr.size
            stats["classes"][theClass.name]["size"] += file_attr.size

        if save:
            self._print_file_attr("|".join(save), file_attr, f)

        # Report progress
        if self.config.table and stats["nFile"] % 1000 == 0:
            delta = int(time.time() - self.start)
            sys.stdout.write(
                "Elapsed time: %02d:%02d:%02d, number of files processed: %d, rate: %.1f [files/sec]\r"
                % (
                    delta // 3600,
                    (delta % 3600) // 60,
                    delta % 60,
                    stats["nFile"],
                    stats["nFile"] / delta if delta > 0 else 0,
                )
            )
            sys.stdout.flush()

    def _print_file_attr(self, theClass, fn, f):
        print(
            "\t".join(
                [
                    theClass,
                    fn.relative_path,
                    fn.target if fn.target else "",
                    str(fn.resolved),
                    str(fn.size),
                    str(fn.dangling),
                    str(fn.outside),
                ]
            ),
            file=f,
        )


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive find-file``."""
    return ArchiveSummaryCommand.setup_argparse(parser)
