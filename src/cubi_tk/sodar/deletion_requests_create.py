import argparse
from pathlib import PurePosixPath
import os

from loguru import logger

from cubi_tk.api_models import iRODSDataObject
from cubi_tk.exceptions import CubiTkException
from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi


class SodarDeletionRequestsCommand:

    def __init__(self, args):
        # Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-remote`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "irods_paths",
            nargs='+',
            help=(
                "Paths to files or collections in irods that should get a deletion request. Relative paths will be "
                "taken in relation to the assay base path. Non-recursive wildcards (?/*) can be used, if literal strings are given."
            ),
        )
        parser.add_argument(
            '-c',
            "--collections",
            nargs='+',
            help=(
                "White list of base collections (samples), all files not matching these will not get a deletion request."
            ),
        )
        parser.add_argument(
            '-d',
            "--description",
            default=None,
            help=(
                "Text description to be added to the deletion requests"
            ),
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> int:
        """Entry point into the command."""
        return cls(args).execute()

    def execute(self) -> int:
        """Execute the SodarAPI calls to ."""

        res = 0
        #res = self.check_args(self.args)
        #if res:  # pragma: nocover
        #    return res

        logger.info("Starting cubi-tk sodar deletion-requests-create")
        print_args(self.args)
        # Initiate API connection, select assay
        sodar_api = SodarApi(self.args)
        assay, study = sodar_api.get_assay_from_uuid()
        # Find all remote files
        irods_files = sodar_api.get_samplesheet_file_list()

        deletion_request_paths = self.gather_deletion_request_paths(irods_files, assay.irods_path)
        for path in deletion_request_paths:
            if self.args.dry_run:
                logger.info(f'DRY-RUN: Would create irods deletion request: {path}')
                continue
            res = sodar_api.post_samplesheet_request_create(path, self.args.descritpion)
            #UNSURE: break or continue on error?
            if res:
                logger.debug(f'Project UUID: {sodar_api.project_uuid}; Assay UUID: {sodar_api.assay_uuid}')
                raise CubiTkException(f'Could not create irods deletion request: {path}')

        logger.info("All done.")
        return 0

    def gather_deletion_request_paths(self, irods_files: list[iRODSDataObject], assay_path: str) -> list[str]:
        """Gather all paths for which deletion requests should be created."""

        given_path_patterns = [
            p if p.startswith('/') else os.path.join(assay_path, p) for p in self.args.irods_paths
        ]
        if any('**' in p for p in given_path_patterns):
            logger.warning(
                "The recusrive '**' wildcard is not supported will behave like a '*' instead (non-recursive)."
            )


        # Deletion requests can be made equally for files and collectons, so we do not need to distinguish
        existing_object_paths = {
            PurePosixPath(obj.path) for obj in irods_files
        }
        matched_objects = set()

        for pattern in given_path_patterns:
            # Note: from py3.13 could also use pathlib.PurePath.full_match here, which supports recursive **
            matches = {pp for pp in existing_object_paths if pp.match(pattern)}
            existing_object_paths -= matches
            matched_objects |= matches

        # apply collection whitelist, if given
        if self.args.collections:
            matched_objects = {pp for pp in matched_objects if any(
                pp.is_relative_to(PurePosixPath(assay_path) / coll) for coll in self.args.collections
            )}

        return sorted(map(str, matched_objects))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-remote``."""
    return SodarDeletionRequestsCommand.setup_argparse(parser)
