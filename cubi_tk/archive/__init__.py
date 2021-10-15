"""``cubi-tk archive``: tools for archive projects (to the CEPH system, for example)

Available Commands
------------------

``summary``
    Lists files that might be problematic for archival (symlinks & large files)
``prepare``
    prepare archive: checks presence of README, compress .snakemake & others
``copy``
    perform archival: copies the prepared output to its final destination, with hashdeep audit

More Information
----------------

- Also see ``cubi-tk archive`` :ref:`cli_main <CLI documentation>` and ``cubi-tk archive --help`` for more information.

"""

import argparse

from ..common import run_nocmd
from .copy import setup_argparse as setup_argparse_copy
from .prepare import setup_argparse as setup_argparse_prepare
from .summary import setup_argparse as setup_argparse_summary


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for archive command."""
    subparsers = parser.add_subparsers(dest="archive_cmd")

    setup_argparse_copy(subparsers.add_parser("copy", help="Perform archival (copy and audit)"))
    setup_argparse_prepare(
        subparsers.add_parser("prepare", help="Prepare the project directory for archival")
    )
    setup_argparse_summary(
        subparsers.add_parser(
            "summary", help="Collects a summary of files in the project directory"
        )
    )


def run(args, parser, subparser):
    """Main entry point for archive command."""
    if not args.archive_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.archive_cmd(args, parser, subparser)
