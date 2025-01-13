"""``cubi-tk irods``: iRODS command line interface.

Sub Commands
------------

``check``
    Check target iRODS collection (all md5 files? metadata md5 consistent? enough replicas?).

More Information
----------------

Also see ``cubi-tk irods`` CLI documentation and ``cubi-tk irods --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .check import setup_argparse as setup_argparse_check


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for irods command."""
    subparsers = parser.add_subparsers(dest="irods_cmd")

    setup_argparse_check(
        subparsers.add_parser(
            "check",
            help="Check target iRODS collection (all MD5 files? metadata MD5 consistent? enough replicas?).",
        )
    )


def run(args, parser, subparser):
    """Main entry point for iRODS command."""
    if not args.irods_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.irods_cmd(args, parser, subparser)
