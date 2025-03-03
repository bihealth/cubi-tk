"""Main entry point for CUBI-TK"""
# PYTHON_ARGCOMPLETE_OK

import argparse
import os
import sys

import argcomplete
from loguru import logger

from cubi_tk import __version__
from cubi_tk.parsers import get_basic_parser

from .common import run_nocmd
from .irods import run as run_irods
from .irods import setup_argparse as setup_argparse_irods
from .isa_tab import run as run_isa_tab
from .isa_tab import setup_argparse as setup_argparse_isa_tab
from .isa_tpl import run as run_isa_tpl
from .isa_tpl import setup_argparse as setup_argparse_isa_tpl
from .org_raw import run as run_org_raw
from .org_raw import setup_argparse as setup_argparse_org_raw
from .sea_snap import run as run_sea_snap
from .sea_snap import setup_argparse as setup_argparse_sea_snap
from .snappy import run as run_snappy
from .snappy import setup_argparse as setup_argparse_snappy
from .sodar import run as run_sodar
from .sodar import setup_argparse as setup_argparse_sodar


def setup_argparse_only():  # pragma: nocover
    """Wrapper for ``setup_argparse()`` that only returns the parser.

    Only used in sphinx documentation via ``sphinx-argparse``.
    """
    return setup_argparse()[0]


def setup_argparse():
    """Create argument parser."""
    # Construct argument parser and set global options.
    basic_parser =get_basic_parser()
    parser = argparse.ArgumentParser(prog="cubi-tk", parents=[basic_parser])
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
        subparsers.add_parser("snappy", help="Tools for supporting the SNAPPY pipeline."),
    )
    setup_argparse_sodar(subparsers.add_parser("sodar", help="SODAR command line interface."))
    setup_argparse_irods(subparsers.add_parser("irods", help="iRods command line interface."))
    setup_argparse_org_raw(subparsers.add_parser("org-raw", help="org_raw command line interface."))
    setup_argparse_sea_snap(
        subparsers.add_parser("sea-snap", help="Tools for supporting the RNA-SeASnaP pipeline.")
    )

    return parser, subparsers


def main(argv=None):
    """Main entry point before parsing command line arguments."""
    # Setup command line parser.
    parser, subparsers = setup_argparse()
    argcomplete.autocomplete(parser)

    # Actually parse command line arguments.
    args = parser.parse_args(argv)

    # Setup logging verbosity.
    logger.remove()
    if args.verbose:  # pragma: no cover
        #will use default formatter "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        logger.add(sys.stdout, level="DEBUG")
    else:
        logger.add(sys.stdout, format="<level>{level:1.1}</level> - <green>{time:DD.MM.YYYY HH:mm:ss}</green> - <level>{message}</level> ", level="INFO")
    

    # Handle the actual command line.
    cmds = {
        None: run_nocmd,
        "isa-tpl": run_isa_tpl,
        "isa-tab": run_isa_tab,
        "snappy": run_snappy,
        "sea-snap": run_sea_snap,
        "sodar": run_sodar,
        "irods": run_irods,
        "org-raw": run_org_raw,
    }

    res = cmds[args.cmd](args, parser, subparsers.choices[args.cmd] if args.cmd else None)
    if not res:
        logger.success("All done. Have a nice day!")
    else:  # pragma: nocover
        logger.error("Something did not work out correctly.")
    return res


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main(sys.argv))
