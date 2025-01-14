"""``cubi-tk sodar``: SODAR command line interface.

Sub Commands
------------

``download-sheet``
    Download ISA-tab sheet from SODAR.

``upload-sheet`` (planned)
    Upload ISA-tab sheet to SODAR.

``landing-zone-list``
    Create a landing zone.

``landing-zone-details`` (planned)
    Show details for landing zone.

``landing-zone-create``
    Create a landing zone.

``landing-zone-validate``
    Validate a landing zone.

``landing-zone-move``
    Move a landing zone.

``landing-zone-delete`` (planned)
    Delete a landing zone.

``add-ped``
    Download sample sheet, add rows from PED file, and re-upload.

``update-samplesheet``
    Directly update ISA sample sheet (without intermediate files), based on ped file &/ command line specified data.

``pull-data-collection``
    Download data collection from iRODS.

``pull-raw-data``
    Download raw data from iRODS for samples from the sample sheet.

``ingest-fastq``
    Upload external files to SODAR
    (defaults for fastq files).

``ingest``
    Upload arbitrary files to SODAR

``check-remote``
    Check if or which local files with md5 sums are already deposited in iRODs/Sodar

More Information
----------------

Also see ``cubi-tk sodar`` CLI documentation and ``cubi-tk sodar --help`` for more
information.
"""

import argparse

from ..common import run_nocmd
from .add_ped import setup_argparse as setup_argparse_add_ped
from .check_remote import setup_argparse as setup_argparse_check_remote
from .download_sheet import setup_argparse as setup_argparse_download_sheet
from .ingest import setup_argparse as setup_argparse_ingest
from .ingest_fastq import setup_argparse as setup_argparse_ingest_fastq
from .lz_create import setup_argparse as setup_argparse_lz_create
from .lz_list import setup_argparse as setup_argparse_lz_list
from .lz_move import setup_argparse as setup_argparse_lz_move
from .lz_validate import setup_argparse as setup_argparse_lz_validate
from .pull_data_collection import setup_argparse as setup_argparse_pull_data_collection
from .pull_raw_data import setup_argparse as setup_argparse_pull_raw_data
from .update_samplesheet import setup_argparse as setup_argparse_update_samplesheet
from .upload_sheet import setup_argparse as setup_argparse_upload_sheet


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for sodar command."""
    subparsers = parser.add_subparsers(dest="sodar_cmd")

    setup_argparse_add_ped(
        subparsers.add_parser("add-ped", help="Augment sample sheet from PED file")
    )
    setup_argparse_update_samplesheet(
        subparsers.add_parser("update-samplesheet", help="Update sample sheet")
    )
    setup_argparse_download_sheet(subparsers.add_parser("download-sheet", help="Download ISA-tab"))
    setup_argparse_upload_sheet(
        subparsers.add_parser("upload-sheet", help="Upload and replace ISA-tab")
    )
    setup_argparse_pull_data_collection(
        subparsers.add_parser("pull-data-collection", help="Download data collections from iRODS")
    )
    setup_argparse_pull_raw_data(
        subparsers.add_parser("pull-raw-data", help="Download raw data from iRODS")
    )
    setup_argparse_lz_create(
        subparsers.add_parser("landing-zone-create", help="Creating landing zone")
    )
    setup_argparse_lz_list(subparsers.add_parser("landing-zone-list", help="List landing zones"))
    setup_argparse_lz_move(
        subparsers.add_parser("landing-zone-move", help="Submit landing zone for moving")
    )
    setup_argparse_lz_validate(
        subparsers.add_parser("landing-zone-validate", help="Submit landing zone for validation")
    )
    setup_argparse_ingest_fastq(
        subparsers.add_parser(
            "ingest-fastq", help="Upload external files to SODAR (defaults for fastq)"
        )
    )
    setup_argparse_ingest(subparsers.add_parser("ingest", help="Upload arbitrary files to SODAR"))
    setup_argparse_check_remote(
        subparsers.add_parser(
            "check-remote", help="Compare local files with md5 sum against SODAR/iRODS"
        )
    )


def run(args, parser, subparser):
    """Main entry point for isa-tpl command."""
    if not args.sodar_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.sodar_cmd(args, parser, subparser)
