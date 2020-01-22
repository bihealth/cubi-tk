"""Common code."""

import fcntl
import glob
import os
import struct
import sys
import termios
import warnings
from subprocess import check_output, CalledProcessError
from uuid import UUID

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
