"""``cubi-tk sea-snap``: tools for supporting the Sea-snap pipeline.

Available Commands
------------------

``check-irods``
    Check consistency of sample info, blueprint and files on SODAR.
``itransfer-raw-data``
    Transfer raw data from ``work/input_links`` directory of ``ngs_mapping``.
``itransfer-results``
    Transfer results and logs from ``output`` directory.
``write-sample-info``
    Pull information from SODAR, parse and write sample info.

More Information
----------------

- Also see ``cubi-tk sea-snap`` :ref:`cli_main <CLI documentation>` and ``cubi-tk sea-snap --help`` for more information.

"""

import argparse

from cubi_tk.parsers import get_basic_parser, get_sodar_parser, get_sodar_ingest_parser

from ..common import run_nocmd
from .check_irods import setup_argparse as setup_argparse_check_irods
from .itransfer_raw_data import setup_argparse as setup_argparse_itransfer_raw_data
from .itransfer_results import (
    setup_argparse as setup_argparse_itransfer_mapping_results,
)

from .working_dir import setup_argparse as setup_argparse_working_dir
from .write_sample_info import setup_argparse as setup_argparse_write_sample_info


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    basic_parser = get_basic_parser()
    sodar_parser = get_sodar_parser()
    sodar_ingest_parser_no_dest = get_sodar_ingest_parser(include_dest=False)
    subparsers = parser.add_subparsers(dest="sea_snap_cmd")

    setup_argparse_itransfer_raw_data(
        subparsers.add_parser("itransfer-raw-data", parents=[basic_parser, sodar_parser], help="Transfer FASTQs into iRODS landing zone")
    )

    setup_argparse_itransfer_mapping_results(
        subparsers.add_parser(
            "itransfer-results", parents=[basic_parser, sodar_ingest_parser_no_dest], help="Transfer mapping results into iRODS landing zone"
        )
    )
    setup_argparse_working_dir(
        subparsers.add_parser("working-dir", parents=[basic_parser,], help="Create working directory")
    )

    setup_argparse_write_sample_info(
        subparsers.add_parser("write-sample-info", parents=[basic_parser, sodar_parser], help="Generate sample info file")
    )

    setup_argparse_check_irods(
        subparsers.add_parser(
            "check-irods", parents=[basic_parser,], help="DEPRECATING! Check consistency of sample info, blueprint and files on SODAR"
        )
    )


def run(args, parser, subparser):
    """Main entry point for sea-snap command."""
    if not args.sea_snap_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.sea_snap_cmd(args, parser, subparser)
