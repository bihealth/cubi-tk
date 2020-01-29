"""Main entry point for CUBI-SAK"""

import argparse
import logging
import sys

import logzero
from logzero import logger

from cubi_sak import __version__
from .common import run_nocmd
from .isa_tab import run as run_isa_tab
from .isa_tab import setup_argparse as setup_argparse_isa_tab
from .isa_tpl import run as run_isa_tpl
from .isa_tpl import setup_argparse as setup_argparse_isa_tpl
from .snappy import run as run_snappy
from .snappy import setup_argparse as setup_argparse_snappy


def setup_argparse_only():  # pragma: nocover
    """Wrapper for ``setup_argparse()`` that only returns the parser.

    Only used in sphinx documentation via ``sphinx-argparse``.
    """
    return setup_argparse()[0]


def setup_argparse():
    """Create argument parser."""
    # Construct argument parser and set global options.
    parser = argparse.ArgumentParser(prog="cubi-sak")
    parser.add_argument("--verbose", action="store_true", default=False, help="Increase verbosity.")
    parser.add_argument("--version", action="version", version="%%(prog)s %s" % __version__)

    # Add sub parsers for each argument.
    subparsers = parser.add_subparsers(dest="cmd")

    setup_argparse_isa_tpl(
        subparsers.add_parser(
            "isa-tpl", help="Create of ISA-tab directories from predefined templates."
        )
    )
    setup_argparse_isa_tab(
        subparsers.add_parser("isa-tab", help="ISA-tab tools besides templating.")
    )
    setup_argparse_snappy(
        subparsers.add_parser("snappy", help="Tools for supporting the SNAPPY pipeline.")
    )

    return parser, subparsers


def main(argv=None):
    """Main entry point before parsing command line arguments."""
    # Setup command line parser.
    parser, subparsers = setup_argparse()

    # Actually parse command line arguments.
    args = parser.parse_args(argv)

    # Setup logging verbosity.
    if args.verbose:  # pragma: no cover
        level = logging.DEBUG
    else:
        level = logging.INFO
    logzero.loglevel(level=level)

    # Handle the actual command line.
    cmds = {None: run_nocmd, "isa-tpl": run_isa_tpl, "isa-tab": run_isa_tab, "snappy": run_snappy}

    res = cmds[args.cmd](args, parser, subparsers.choices[args.cmd] if args.cmd else None)
    if not res:
        logger.info("All done. Have a nice day!")
    else:  # pragma: nocover
        logger.error("Something did not work out correctly.")
    return res


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv))
