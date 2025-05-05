"""``cubi-tk isa-tab``: ISA-tab tooling.

Sub Commands
------------

``validate``
    Validate ISA-tab files for correctness and perform sanity checks.

``add-ped``
    Given a germline DNA sequencing ISA-tab file and a PED file, add new lines to the ISA-tab
    file and update existing ones, e.g., for newly added parents.

More Information
----------------

Also see ``cubi-tk isa-tab`` CLI documentation and ``cubi-tk isa-tab --help`` for more
information.
"""

import argparse

from cubi_tk.parsers import get_basic_parser, get_sodar_parser

from ..common import run_nocmd
from .add_ped import setup_argparse as setup_argparse_add_ped
from .validate import setup_argparse as setup_argparse_validate


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    basic_parser = get_basic_parser()
    sodar_parser = get_sodar_parser()
    subparsers = parser.add_subparsers(dest="isa_tab_cmd")

    setup_argparse_add_ped(
        subparsers.add_parser("add-ped", parents=[basic_parser, sodar_parser], help="Add records from PED file to ISA-tab")
    )
    setup_argparse_validate(subparsers.add_parser("validate", parents=[basic_parser, sodar_parser], help="Validate ISA-tab"))


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.isa_tab_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.isa_tab_cmd(args, parser, subparser)
