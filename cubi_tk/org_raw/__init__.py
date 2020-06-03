"""``cubi-tk org-raw``: raw data tooling

Sub Commands
------------

``check``
    Perform common FASTQ checks: check integrity of gzip archive, existence of MD5 file (or check
    for consistency with MD5 file).

``organize``
    Move FASTQ files based on patterns and also perform checks.

More Information
----------------

Also see ``cubi-tk org-raw`` CLI documentation and ``cubi-tk org-raw`` for more information.
"""

import argparse

from ..common import run_nocmd
from .check import setup_argparse as setup_argparse_check
from .organize import setup_argparse as setup_argparse_organize


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for org-raw command."""
    subparsers = parser.add_subparsers(dest="org_raw_cmd")

    setup_argparse_check(subparsers.add_parser("check", help="Check consistency of raw data"))
    setup_argparse_organize(subparsers.add_parser("organize", help="Check consistency of raw data"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.org_raw_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.org_raw_cmd(args, parser, subparser)
