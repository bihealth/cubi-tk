"""``cubi-tk archive``: common features"""

import argparse
import attr
import json
import os
import subprocess
import sys
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
    """Returns attributes of the file named `filename`.

    The attributes are:
    - relative_path: the file path relative to directory `relative_to`
    - resolved: the resolved path (i.e. normalised absolute path to the file)
    - symlink: True if the file is a symlink, False otherwise
    - dangling: True if the symlink's target cannot be read (missing or permissions),
      False the filename is not a symlink, or if the target can be read
    - outside: True if the file is not in the `relative_to` directory
    - target: the symlink target, or None if filename isn't a symlink
    - size: the size of the file, or of its target if the file is a symlink.
      If the file is a dangling symlink, the size is set to 0
    """
    resolved = Path(filename).resolve(strict=False)
    symlink = os.path.islink(filename)
    if symlink:
        target = os.readlink(filename)
        try:
            dangling = not resolved.exists()
        except PermissionError:
            dangling = None
        if dangling is None or dangling:
            size = 0
        else:
            size = resolved.stat().st_size
    else:
        dangling = False
        outside = False
        target = None
        size = resolved.stat().st_size
    outside = os.path.relpath(resolved, start=relative_to).startswith("../")
    return FileAttributes(
        relative_path=os.path.relpath(filename, start=relative_to),
        resolved=resolved,
        symlink=symlink,
        dangling=dangling,
        outside=outside,
        target=target,
        size=size,
    )


def traverse_project_files(directory, followlinks=True):
    root = Path(directory).resolve(strict=True)
    for path, _, files in os.walk(root, followlinks=followlinks):
        for filename in files:
            yield get_file_attributes(os.path.join(path, filename), root)


def load_variables(template_dir):
    """
    :param template_dir: Path to cookiecutter directory.
    :type template_dir: str

    :return: Returns load variables found in the cokiecutter template directory.
    """
    config_path = os.path.join(template_dir, "cookiecutter.json")
    with open(config_path, "rt", encoding="utf8") as inputf:
        result = json.load(inputf)
    return result


def run_hashdeep(directory, out_file=None, num_threads=4, ref_file=None):
    """Run hashdeep recursively on directory, following symlinks, stores the result in out_file.
    Hashdeep can be run in normal or audit mode, when ref_file is provided."""
    # Output of out_file of stdout
    if out_file:
        f = open(out_file, "wt")
    else:
        f = sys.stdout
    # hashdeep command for x or for audit
    cmd = ["hashdeep", "-j", str(num_threads), "-l", "-r"]
    if ref_file:
        cmd += ["-vvv", "-a", "-k", ref_file, "."]
    else:
        cmd += ["-o", "fl", "."]
    # Run hashdeep from the directory, storing the output in f
    p = subprocess.Popen(cmd, cwd=directory, encoding="utf-8", stdout=f, stderr=subprocess.PIPE)
    p.communicate()
    # Return hashdeep return value
    if out_file:
        f.close()
    return p.returncode


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk archive``."""
    return ArchiveCommandBase.setup_argparse(parser)
