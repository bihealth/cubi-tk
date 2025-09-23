"""``cubi-tk sodar``: SODAR command line interface.

Sub Commands
------------

``add-ped``
    Download sample sheet, add rows from PED file, and re-upload.

``check-remote``
    Check if or which local files with checksums are already deposited in iRODs/Sodar

``deletion-requests``
    Create deletion requests for files in SODAR/iRODs

``download-sheet``
    Download ISA-tab sheet from SODAR.

``ingest-collection``
    Upload arbitrary files to a single collection in SODAR

``ingest-data``
    Upload files to SODAR and match them to collections
    (defaults for fastq files).

``create-landingzone``
    Create a landing zone for a SODAR Project.

``list-landingzone``
    List existing landing zones for a SODAR Project.

``landing-zone-validate``
    Validate a landing zone.

``landing-zone-move``
    Move a landing zone.

``pull-data``
    Download data from iRODS.

``pull-raw-data``
    DEPRECATING! Download raw data from iRODS for samples from the sample sheet.

``update-samplesheet``
    Directly update ISA sample sheet (without intermediate files), based on ped file &/ command line specified data.

``upload-sheet``
    Upload ISA-tab sheet to SODAR.


More Information
----------------

Also see ``cubi-tk sodar`` CLI documentation and ``cubi-tk sodar --help`` for more
information.
"""

import argparse

from cubi_tk.parsers import get_basic_parser, get_sodar_parser, get_sodar_ingest_parser

from ..common import run_nocmd
from .add_ped import setup_argparse as setup_argparse_add_ped
from .check_remote import setup_argparse as setup_argparse_check_remote
from .deletion_requests_create import setup_argparse as setup_argparse_deletion_requests
from .download_sheet import setup_argparse as setup_argparse_download_sheet
from .ingest_collection import setup_argparse as setup_argparse_ingest_collection
from .ingest_data import setup_argparse as setup_argparse_ingest_data
from .lz_create_landingzone import setup_argparse as setup_argparse_create_landingzone
from .lz_list_landingzones import setup_argparse as setup_argparse_list_landingzones
from .lz_move import setup_argparse as setup_argparse_lz_move
from .lz_validate import setup_argparse as setup_argparse_lz_validate
from .pull_data import setup_argparse as setup_argparse_pull_data
from .pull_raw_data import setup_argparse as setup_argparse_pull_raw_data
from .update_samplesheet import setup_argparse as setup_argparse_update_samplesheet
from .upload_sheet import setup_argparse as setup_argparse_upload_sheet


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Main entry point for sodar command."""
    basic_parser = get_basic_parser()
    sodar_parser_project_uuid = get_sodar_parser(with_dest=True)
    sodar_parser_project_uuid_assay_uuid = get_sodar_parser(with_dest=True, with_assay_uuid=True)
    sodar_ingest_parser = get_sodar_ingest_parser()
    sodar_ingest_parser_no_dest = get_sodar_ingest_parser(include_dest=False)
    sodar_parser_lz_uuid = get_sodar_parser(
        with_dest=True,
        dest_string="landing_zone_uuid",
        dest_help_string="UUID of Landing Zone to move.",
    )

    subparsers = parser.add_subparsers(dest="sodar_cmd")

    setup_argparse_add_ped(
        subparsers.add_parser(
            "add-ped",
            parents=[basic_parser, sodar_parser_project_uuid],
            help="Augment sample sheet from PED file",
        )
    )
    setup_argparse_check_remote(
        subparsers.add_parser(
            "check-remote",
            parents=[basic_parser, sodar_parser_project_uuid_assay_uuid],
            help="Compare local files with checksum against SODAR/iRODS",
        )
    )
    setup_argparse_download_sheet(
        subparsers.add_parser(
            "download-sheet",
            parents=[basic_parser, sodar_parser_project_uuid],
            help="Download ISA-tab",
        )
    )
    setup_argparse_deletion_requests(
        subparsers.add_parser(
            "deletion-requests",
            parents=[basic_parser, sodar_parser_project_uuid_assay_uuid],
            help="Create deletion reuqests for SODAR/iRODS",
        )
    )
    setup_argparse_ingest_data(
        subparsers.add_parser(
            "ingest-data",
            parents=[basic_parser, sodar_ingest_parser],
            help="Upload files to SODAR project",
        )
    )
    setup_argparse_ingest_collection(
        subparsers.add_parser(
            "ingest-collection",
            parents=[basic_parser, sodar_ingest_parser_no_dest],
            help="Upload a set of arbitrary files to a single iRODS colelction from SODAR",
        )
    )
    setup_argparse_pull_data(
        subparsers.add_parser(
            "pull-data",
            parents=[basic_parser, sodar_parser_project_uuid_assay_uuid],
            help="Download data from iRODS",
        )
    )
    setup_argparse_pull_raw_data(
        subparsers.add_parser(
            "pull-raw-data",
            parents=[basic_parser, sodar_parser_project_uuid_assay_uuid],
            help="DEPRECATING! Download raw data from iRODS",
        )
    )
    setup_argparse_create_landingzone(
        subparsers.add_parser(
            "create-landingzone",
            parents=[basic_parser, sodar_parser_project_uuid_assay_uuid],
            help="Creating landing zone for project",
        )
    )
    setup_argparse_list_landingzones(
        subparsers.add_parser(
            "list-landingzones",
            parents=[basic_parser, sodar_parser_project_uuid],
            help="List landing zones of project",
        )
    )
    setup_argparse_lz_move(
        subparsers.add_parser(
            "landing-zone-move",
            parents=[basic_parser, sodar_parser_lz_uuid],
            help="Submit given landing zone for moving",
        )
    )
    setup_argparse_lz_validate(
        subparsers.add_parser(
            "landing-zone-validate",
            parents=[basic_parser, sodar_parser_lz_uuid],
            help="Submit given landing zone for validation",
        )
    )
    setup_argparse_update_samplesheet(
        subparsers.add_parser(
            "update-samplesheet",
            parents=[basic_parser, get_sodar_parser(with_dest=True)],
            help="Update sample sheet",
        )
    )
    setup_argparse_upload_sheet(
        subparsers.add_parser(
            "upload-sheet",
            parents=[basic_parser, sodar_parser_project_uuid],
            help="Upload and replace ISA-tab",
        )
    )


def run(args, parser, subparser):
    """Main entry point for sodar command."""
    if not args.sodar_cmd:  # pragma: nocover
        return run_nocmd(args, parser, subparser)
    else:
        return args.sodar_cmd(args, parser, subparser)
