"""``cubi-tk dkfz``: tools for uploading data from the DKFZ sequencing center in Heidelberg

Available Commands
------------------

``prepare-isatab``
    Writes the investigation, sample & assay ISATAB files for inspection.
``ingest-fastq``
    Transfer raw data from the DKFZ download directory structure.

More Information
----------------

- Also see ``cubi-tk dktk`` :ref:`cli_main <CLI documentation>` and ``cubi-tk dktk --help`` for more information.

"""

import argparse

from ..common import run_nocmd
from .common import setup_argparse as setup_argparse_dkfz
from .prepare_isatab import setup_argparse as setup_argparse_prepare_isatab
from .ingest_fastq import setup_argparse as setup_argparse_ingest_fastq


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for dkfz command."""
    subparsers = parser.add_subparsers(dest="dkfz_cmd")

    setup_argparse_prepare_isatab(
        subparsers.add_parser(
            "prepare-isatab", help="Create ISA-Tab files from the dataset metadata"
        )
    )
    setup_argparse_ingest_fastq(
        subparsers.add_parser(
            "ingest-fastq", help="Transfer FASTQs into iRODS landing zone"
        )
    )


def run(args, parser, subparser):
    """Main entry point for dkfz command."""
    if not args.dkfz_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.dkfz_cmd(args, parser, subparser)
