"""Common code."""

import difflib
import fcntl
import glob
import os
import pathlib
import shutil
import struct
import sys
import tempfile
import termios
import typing
import warnings
from subprocess import check_output, CalledProcessError
from uuid import UUID

import icdiff
from logzero import logger
from termcolor import colored

from .exceptions import IrodsIcommandsUnavailableException, IrodsIcommandsUnavailableWarning


def run_nocmd(_, parser, subparser=None):  # pragma: no cover
    """No command given, print help and ``exit(1)``."""
    if subparser:
        subparser.print_help()
        subparser.exit(1)
    else:
        parser.print_help()
        parser.exit(1)


def yield_files_recursively(path, print_=False, file=sys.stderr):
    """Recursively yield below path to ``file`` in sorted order, print optionally"""
    while len(path) > 1 and path[-1] == "/":  # trim trailing slashes
        path = path[:-1]  # pragma: no cover
    paths = glob.glob(os.path.join(path, "**"))
    for p in sorted(paths):
        p = p[len(path) + 1 :]
        if print_:
            print(p, file=file)  # pragma: no cover
        yield p


def is_uuid(x):
    """Return True if ``x`` is a string and looks like a UUID."""
    try:
        return str(UUID(x)) == x
    except:  # noqa: E722
        return False


def check_irods_icommands(warn_only=True):  # disabled when testing  # pragma: nocover
    executables = ("iinit", "iput", "iget", "irsync")
    missing = []
    for prog in executables:
        try:
            check_output(["which", prog])
        except CalledProcessError:
            missing.append(prog)

    if missing:
        msg = "Could not find irods-icommands executables: %s", ", ".join(missing)
        if warn_only:
            warnings.warn(msg, IrodsIcommandsUnavailableWarning)
        else:
            raise IrodsIcommandsUnavailableException(msg)


def sizeof_fmt(num, suffix="B"):  # pragma: nocover
    """Source: https://stackoverflow.com/a/1094933/84349"""
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def get_terminal_columns():
    """Return number of columns."""

    def ioctl_gwinz(fd):
        try:
            cr = struct.unpack("hh", fcntl.ioctl(fd, termios.TIOCGWINSZ, "1234"))
        except Exception:  # noqa
            return None
        return cr

    cr = ioctl_gwinz(0) or ioctl_gwinz(1) or ioctl_gwinz(2)
    if cr and cr[1] > 0:
        return cr[1]
    else:
        return 80


def overwrite_helper(
    out_path: typing.Union[str, pathlib.Path],
    contents: str,
    *,
    do_write: bool,
    show_diff: bool,
    show_diff_side_by_side: bool = False
) -> None:
    out_path_obj = pathlib.Path(out_path)
    with tempfile.NamedTemporaryFile(mode="w+t") as sheet_file:
        # Write sheet to temporary file.
        sheet_file.write(contents)

        # Compare sheet with output if exists and --show-diff given.
        if show_diff:
            if out_path != "-" and out_path_obj.exists():
                with out_path_obj.open("rt") as inputf:
                    old_lines = inputf.read().splitlines(keepends=False)
            else:
                old_lines = []
            sheet_file.seek(0)
            new_lines = sheet_file.read().splitlines(keepends=False)

            if not show_diff_side_by_side:
                lines = difflib.unified_diff(
                    old_lines, new_lines, fromfile=str(out_path), tofile=str(out_path)
                )
                for line in lines:
                    if line.startswith(("+++", "---")):
                        print(
                            colored(line, color="white", attrs=("bold",)), end="", file=sys.stdout
                        )
                    elif line.startswith("@@"):
                        print(colored(line, color="cyan", attrs=("bold",)), end="", file=sys.stdout)
                    elif line.startswith("+"):
                        print(colored(line, color="green", attrs=("bold",)), file=sys.stdout)
                    elif line.startswith("-"):
                        print(colored(line, color="red", attrs=("bold",)), file=sys.stdout)
                    else:
                        print(line, file=sys.stdout)
            else:
                cd = icdiff.ConsoleDiff(cols=get_terminal_columns(), line_numbers=True)
                lines = cd.make_table(
                    old_lines,
                    new_lines,
                    fromdesc=str(out_path),
                    todesc=str(out_path),
                    context=True,
                    numlines=3,
                )
                for line in lines:
                    line = "%s\n" % line
                    if hasattr(sys.stdout, "buffer"):
                        sys.stdout.buffer.write(line.encode("utf-8"))
                    else:
                        sys.stdout.write(line)

            sys.stdout.flush()
            if not lines:
                logger.info("File %s not changed, no diff...", out_path)

        # Actually copy the file contents.
        if do_write:
            logger.debug("Writing file contents to %s", out_path)
            sheet_file.seek(0)
            if out_path == "-":
                shutil.copyfileobj(sheet_file, sys.stdout)
            else:
                with out_path_obj.open("wt") as output_file:
                    shutil.copyfileobj(sheet_file, output_file)
