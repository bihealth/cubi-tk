"""``cubi-sak snappy``: tools for supporting the SNAPPY pipeline.

Available Commands
------------------

``pull-sheet``
    Pull sample sheet from SODAR and write out to BiomedSheet format.

More Information
----------------

- Also see ``cubi-sak snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-sak snappy --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.

"""

import argparse

from ..common import run_nocmd
from .pull_sheet import setup_argparse as setup_argparse_pull_sheet
from .itransfer_raw_data import setup_argparse as setup_argparse_itransfer_raw_data
from .itransfer_ngs_mapping import setup_argparse as setup_argparse_itransfer_ngs_mapping


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="snappy_cmd")

    setup_argparse_pull_sheet(
        subparsers.add_parser("pull-sheet", help="Pull SODAR sample sheet into biomedsheet")
    )
    setup_argparse_itransfer_raw_data(
        subparsers.add_parser("itransfer-raw-data", help="Transfer FASTQs into iRODS landing zone")
    )
    setup_argparse_itransfer_ngs_mapping(
        subparsers.add_parser(
            "itransfer-ngs-mapping", help="Transfer ngs_mapping results into iRODS landing zone"
        )
    )


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.snappy_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.snappy_cmd(args, parser, subparser)
