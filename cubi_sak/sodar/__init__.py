"""``cubi-sak sodar``: SODAR command line interface.

Sub Commands
------------

``download-sheet``
    Download ISA-tab sheet from SODAR.

``upload-sheet`` (planned)
    Upload ISA-tab sheet to SODAR.

More Information
----------------

Also see ``cubi-sak sodar`` CLI documentation and ``cubi-sak sodar --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .download_sheet import setup_argparse as setup_argparse_download_sheet


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="sodar_cmd")

    setup_argparse_download_sheet(subparsers.add_parser("download", help="Download ISA-tab"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.sodar_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.sodar_cmd(args, parser, subparser)
