"""Common code."""

import glob
import os
import sys


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
