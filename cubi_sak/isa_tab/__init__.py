"""``cubi-sak isa-tab``: ISA-tab tooling.

Sub Commands
------------

``validate``
    Validate ISA-tab files for correctness and perform sanity checks.

``resolve-hpo``
    Resolve lists of HPO terms to TSV suitable for copy-and-paste into ISA-tab.

More Information
----------------

Also see ``cubi-sak isa-tab`` CLI documentation and ``cubi-sak isa-tab --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .validate import setup_argparse as setup_argparse_validate
from .resolve_hpo import setup_argparse as setup_argparse_resolve_hpo


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="isa_tab_cmd")

    setup_argparse_validate(subparsers.add_parser("validate", help="Validate ISA-tab"))
    setup_argparse_resolve_hpo(
        subparsers.add_parser("resolve-hpo", help="Resolve HPO term lists to ISA-tab fragments")
    )


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.isa_tab_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tab_cmd(args, parser, subparser)
