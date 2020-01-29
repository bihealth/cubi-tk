"""``cubi-sak isa-tab``: ISA-tab tooling.

TODO: more docs

More Information
----------------

Also see ``cubi-sak isa-tab`` CLI documentation and ``cubi-sak isa-tab --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .validate import setup_argparse as setup_argparse_validate


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="isa_tab_cmd")

    setup_argparse_validate(subparsers.add_parser("validate", help="Validate ISA-tab"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.isa_tab_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tab_cmd(args, parser, subparser)
