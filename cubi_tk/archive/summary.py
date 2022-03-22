"""``cubi-tk archive summary``: Creates a summary table of problematic files and files of interest"""

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

    classes: str
    table: str


class ArchiveSummaryCommand(common.ArchiveCommandBase):
    """Implementation of archive summary command."""

    command_name = "summary"

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
        parser.add_argument(
            "--dont-follow-links",
            action="store_true",
            help="Do not follow symlinks to directories. Required when the project contains circular symlinks",
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

        if not os.path.exists(self.config.project) or not os.path.isdir(self.config.project):
            logger.error("Illegal project path : '{}'".format(self.config.project))
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Traverse all project files for summary"""
        res = self.check_args(self.config)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk archive summary")
        logger.info("  args: %s", self.config)

        stats = self._init_stats(os.path.normpath(os.path.realpath(self.config.classes)))

        f = open(self.config.table, "wt") if self.config.table else sys.stdout

        # Print output table title lines
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

        # Traverse the project tree to accumulate statistics and populate the output table
        self.start = time.time()
        for file_attr in traverse_project_files(
            self.config.project, followlinks=not self.config.dont_follow_links
        ):
            self._aggregate_stats(file_attr, stats, f)
        f.close()

        # Clear the progress line
        if self.config.table:
            sys.stdout.write(" " * 80 + "\r")
            sys.stdout.flush()

        # Print general overview on the screen
        self._report_stats(stats)

        return 0

    def _report_stats(self, stats):
        logger.info(
            "Number of files in {}: {} ({} outside of project directory)".format(
                self.config.project, stats["nFile"], stats["nOutside"]
            )
        )
        logger.info(
            "Number of links: {} ({} dangling, {} inaccessible (permissions))".format(
                stats["nLink"], stats["nDangling"], stats["nInaccessible"]
            )
        )
        logger.info(
            "Total size: {} ({} in files outside of the directory)".format(
                stats["size"], stats["size_outside"]
            )
        )
        for (name, the_stat) in stats["classes"].items():
            logger.info(
                "Number of {} files: {} (total size: {})".format(
                    name, the_stat["nFile"], the_stat["size"]
                )
            )
            logger.info(
                "Number of {} files outside the projects directory: {} (total size: {})".format(
                    name, the_stat["nOutside"], the_stat["size_outside"]
                )
            )
            logger.info(
                "Number of {} files lost (dangling or inaccessible): {}".format(
                    name, the_stat["nLost"]
                )
            )

    @staticmethod
    def _init_stats(f=None):
        if not f:
            f = sys.stdout
        if isinstance(f, str):
            f = open(f, "rt")

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
        for (name, params) in yaml.safe_load(f).items():
            stats["classes"][name] = {
                "min_size": int(params["min_size"]),
                "pattern": re.compile(params["pattern"]),
                "nFile": 0,
                "size": 0,
                "nLost": 0,
                "nOutside": 0,
                "size_outside": 0,
            }

        return stats

    def _aggregate_stats(self, file_attr, stats, f):
        """Aggregate statistics for one file"""
        save = []

        stats["nFile"] += 1
        stats["size"] += file_attr.size

        if file_attr.outside:
            stats["nOutside"] += 1
            stats["size_outside"] += file_attr.size
            save.append("outside")

        # symlinks
        if file_attr.target:
            stats["nLink"] += 1
            if file_attr.dangling:
                stats["nDangling"] += 1
                save.append("dangling")
            if file_attr.dangling is None:
                stats["nInaccessible"] += 1
                save.append("inaccessible")

        # File classes
        for (name, the_class) in stats["classes"].items():
            if not the_class["pattern"].match(file_attr.relative_path):
                continue
            is_lost = file_attr.target and (file_attr.dangling is None or file_attr.dangling)
            if file_attr.size < the_class["min_size"] and not is_lost:
                continue
            save.append(name)
            the_class["nFile"] += 1
            if is_lost:
                the_class["nLost"] += 1
            else:
                if file_attr.outside:
                    the_class["nOutside"] += 1
                    the_class["size_outside"] += file_attr.size
            the_class["size"] += file_attr.size

        if save:
            self._print_file_attr("|".join(save), file_attr, f)

        # Report progress
        if self.config.table and stats["nFile"] % 1000 == 0:
            delta = int(time.time() - self.start)
            sys.stdout.write(
                "\rElapsed time: %02d:%02d:%02d, number of files processed: %d, rate: %.1f [files/sec]\r"
                % (
                    delta // 3600,
                    (delta % 3600) // 60,
                    delta % 60,
                    stats["nFile"],
                    stats["nFile"] / delta if delta > 0 else 0,
                )
            )
            sys.stdout.flush()

    def _print_file_attr(self, the_class, fn, f):
        """Print one row of the summary table"""
        print(
            "\t".join(
                [
                    the_class,
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
    """Setup argument parser for ``cubi-tk archive summary``."""
    return ArchiveSummaryCommand.setup_argparse(parser)
