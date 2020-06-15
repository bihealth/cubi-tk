"""``cubi-tk snappy``: tools for supporting the SNAPPY pipeline.

Available Commands
------------------

``check``
    Check consistency within sample sheet but also between sample sheet and files.
``itransfer-raw-data``
    Transfer raw data from ``work/input_links`` directory of ``ngs_mapping``.
``itransfer-results``
    Transfer results and logs from ``output`` directory.
``write-sample-info``
    Pull information from SODAR, parse and write sample info.

More Information
----------------

- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy --help`` for more information.
- `SNAPPY Pipeline GitLab Project <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/snappy>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.

"""

import argparse

from ..common import run_nocmd
from .itransfer_raw_data import setup_argparse as setup_argparse_itransfer_raw_data
from .itransfer_results import setup_argparse as setup_argparse_itransfer_mapping_results

# from .pull_isa import setup_argparse as setup_argparse_pull_isa
from .working_dir import setup_argparse as setup_argparse_working_dir
from .write_sample_info import setup_argparse as setup_argparse_write_sample_info


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for isa-tpl command."""
    subparsers = parser.add_subparsers(dest="sea_snap_cmd")

    setup_argparse_itransfer_raw_data(
        subparsers.add_parser("itransfer-raw-data", help="Transfer FASTQs into iRODS landing zone")
    )

    setup_argparse_itransfer_mapping_results(
        subparsers.add_parser(
            "itransfer-results", help="Transfer mapping results into iRODS landing zone"
        )
    )
    setup_argparse_working_dir(
        subparsers.add_parser("working-dir", help="Create working directory")
    )

    setup_argparse_write_sample_info(
        subparsers.add_parser("write-sample-info", help="Generate sample info file")
    )


def run(args, parser, subparser):
    """Main entry point for sea-snap command."""
    if not args.sea_snap_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.sea_snap_cmd(args, parser, subparser)
