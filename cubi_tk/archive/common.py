"""``cubi-tk archive``: common features"""

import argparse
import attr
import os
import typing

from pathlib import Path


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for common archive subcommands."""

    verbose: bool
    config: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    project: str


@attr.s(frozen=True, auto_attribs=True)
class FileAttributes:
    """Attributes for files & symlinks"""

    relative_path: str
    resolved: Path
    symlink: bool
    dangling: bool
    outside: bool
    target: str
    size: int


class ArchiveCommandBase:
    """Implementation of archive subcommands."""

    command_name = ""

    def __init__(self, config: Config):
        self.config = config
        self.project = None

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="archive_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument("project", help="Path of project directory")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        raise NotImplementedError("Must be implemented in derived classes")

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        raise NotImplementedError("Must be implemented in derived classes")

    def execute(self) -> typing.Optional[int]:
        raise NotImplementedError("Must be implemented in derived classes")


def get_file_attributes(filename, relative_to):
    resolved = Path(filename).resolve(strict=False)
    symlink = os.path.islink(filename)
    if symlink:
        target = os.readlink(filename)
        try:
            dangling = not resolved.exists()
        except PermissionError:
            dangling = None
        outside = os.path.relpath(resolved, start=relative_to).startswith("../")
        if dangling is None or dangling:
            size = 0
        else:
            size = resolved.stat().st_size
    else:
        dangling = False
        outside = False
        target = None
        size = resolved.stat().st_size
    return FileAttributes(
        relative_path=os.path.relpath(filename, start=relative_to),
        resolved=resolved,
        symlink=symlink,
        dangling=dangling,
        outside=outside,
        target=target,
        size=size,
    )


def traverse_project_files(directory):
    root = Path(directory).resolve(strict=True)
    for path, _, files in os.walk(root):
        for filename in files:
            yield get_file_attributes(os.path.join(path, filename), root)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive``."""
    return ArchiveCommandBase.setup_argparse(parser)
