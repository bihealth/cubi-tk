"""``cubi-tk snappy``: tools for supporting the SNAPPY pipeline.

Available Commands
------------------

``check``
    Check consistency within sample sheet but also between sample sheet and files.
``itransfer-raw-data``
    Transfer raw data from ``work/input_links`` directory of ``ngs_mapping``.
``itransfer-ngs-mapping``
    Transfer results and logs from ``output`` directory of ``ngs_mapping``.
``itransfer-variant-calling``
    Transfer results and logs from ``output`` directory of ``variant_calling``.
``pull-sheet``
    Pull sample sheet from SODAR and write out to BiomedSheet format.
``pull-raw-data``
    Download raw data from SODAR.
``varfish-upload``
    Upload data into VarFish.

More Information
----------------

- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.

"""

import argparse

from ..common import run_nocmd
from .check import setup_argparse as setup_argparse_check
from .itransfer_raw_data import setup_argparse as setup_argparse_itransfer_raw_data
from .itransfer_ngs_mapping import setup_argparse as setup_argparse_itransfer_ngs_mapping
from .itransfer_variant_calling import setup_argparse as setup_argparse_itransfer_variant_calling
from .pull_sheets import setup_argparse as setup_argparse_pull_sheets
from .pull_raw_data import setup_argparse as setup_argparse_pull_raw_data
from .kickoff import setup_argparse as setup_argparse_kickoff
from .varfish_upload import setup_argparse as setup_argparse_varfish_upload


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="snappy_cmd")

    setup_argparse_check(
        subparsers.add_parser(
            "check", help="Check consistency within sample sheet and between sheet and files"
        )
    )

    setup_argparse_itransfer_raw_data(
        subparsers.add_parser("itransfer-raw-data", help="Transfer FASTQs into iRODS landing zone")
    )
    setup_argparse_itransfer_ngs_mapping(
        subparsers.add_parser(
            "itransfer-ngs-mapping", help="Transfer ngs_mapping results into iRODS landing zone"
        )
    )
    setup_argparse_itransfer_variant_calling(
        subparsers.add_parser(
            "itransfer-variant-calling",
            help="Transfer variant_calling results into iRODS landing zone",
        )
    )

    setup_argparse_pull_sheets(
        subparsers.add_parser("pull-sheets", help="Pull SODAR sample sheets into biomedsheet")
    )
    setup_argparse_pull_raw_data(
        subparsers.add_parser(
            "pull-raw-data", help="Pull raw data from SODAR to SNAPPY dataset raw data directory"
        )
    )

    setup_argparse_varfish_upload(
        subparsers.add_parser("varfish-upload", help="Upload variant analysis results into VarFish")
    )

    setup_argparse_kickoff(subparsers.add_parser("kickoff", help="Kick-off SNAPPY pipeline steps."))


def run(args, parser, subparser):
    """Main entry point for snappy command."""
    if not args.snappy_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.snappy_cmd(args, parser, subparser)
