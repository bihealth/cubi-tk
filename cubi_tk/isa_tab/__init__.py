"""``cubi-tk isa-tab``: ISA-tab tooling.

Sub Commands
------------

``validate``
    Validate ISA-tab files for correctness and perform sanity checks.

``resolve-hpo``
    Resolve lists of HPO terms to TSV suitable for copy-and-paste into ISA-tab.

``add-ped``
    Given a germline DNA sequencing ISA-tab file and a PED file, add new lines to the ISA-tab
    file and update existing ones, e.g., for newly added parents.

``annotate``
    Add annotation to an ISA-tab file, given a CSV

More Information
----------------

Also see ``cubi-tk isa-tab`` CLI documentation and ``cubi-tk isa-tab --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .add_ped import setup_argparse as setup_argparse_add_ped
from .resolve_hpo import setup_argparse as setup_argparse_resolve_hpo
from .validate import setup_argparse as setup_argparse_validate
from .annotate import setup_argparse as setup_argparse_annotate


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="isa_tab_cmd")

    setup_argparse_add_ped(
        subparsers.add_parser("add-ped", help="Add records from PED file to ISA-tab")
    )
    setup_argparse_resolve_hpo(
        subparsers.add_parser("resolve-hpo", help="Resolve HPO term lists to ISA-tab fragments")
    )
    setup_argparse_annotate(
        subparsers.add_parser("annotate", help="Add annotation from CSV file to ISA-tab")
    )
    setup_argparse_validate(subparsers.add_parser("validate", help="Validate ISA-tab"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.isa_tab_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tab_cmd(args, parser, subparser)
