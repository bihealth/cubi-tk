"""``cubi-tk sodar check-remote``: check that files are present in remote SODAR/iRODS.

This tool searches recursively for all files with associated checksums files in a given directory (or PWD)
and compares them against files linked to a given SODAR project/irods directory, also the checksums
recorded in local checksum files can be rechecked. No hash format other than md5 and sha256 is currently supported.
Comparison is done based (first) on file names and then (unless disabled) on checksums,
therefore wiles with different names but the same checksum will not be matched.
File are sorted into:
- those present only locally
- those present both locally and in the remote directory
- those present only in the remote directory
"""

import argparse
from collections import defaultdict
import os
from pathlib import Path
import re
import typing

import attr
from loguru import logger

from cubi_tk.irods_common import HASH_SCHEMES
from cubi_tk.parsers import print_args

from ..common import compute_checksum
from ..exceptions import FileChecksumMismatchException
from ..snappy.check_remote import Checker as SnappyChecker
from ..sodar_common import RetrieveSodarCollection


# Adapted from snappy.retrieve_irods_collection
@attr.s(frozen=True, auto_attribs=True)
class FileDataObject:
    """File data object - simple container to keep track of files information and allow comparison"""

    file_name: str
    file_path: str
    file_checksum: str


class FindLocalChecksumFiles:
    """Class contains methods to find local files with associated checksums"""

    def __init__(self, base_path, hash_scheme, recheck_checksum=False, regex_pattern=None):
        """Constructor: init vars"""

        self.searchpath = Path(base_path)
        self.recheck_checksum = recheck_checksum
        self.hash_scheme = hash_scheme
        self.regex_pattern = re.compile(regex_pattern) if regex_pattern else None

    # Adapted from snappy check remote
    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries:
        key: directory path ; value: list of FileDataObject (one per file in that path)
        """
        logger.info("Starting raw data files search ...")

        # Initialise variables
        rawdata_structure_dict = defaultdict(list)

        # Find all checksum files
        checksum_files = self.searchpath.rglob("*." + self.hash_scheme.lower())

        # Check that corresponding files exist
        for checksumfile in checksum_files:
            datafile = checksumfile.with_suffix("")
            if not datafile.exists():
                logger.warning(
                    f"Ignoring orphaned local checksum file: {checksumfile}.\nExpected associated file not found: {datafile}"
                )
                continue
            elif self.regex_pattern and not re.search(self.regex_pattern, str(datafile)):
                logger.debug(
                    f"Skipping {datafile} as it does not match regex: {self.regex_pattern}"
                )
                continue

            with open(checksumfile, "r", encoding="utf8") as f:
                checksum = f.readline()
                # Expected format example:
                # `459db8f7cb0d3a23a38fdc98286a9a9b  out.vcf.gz`
                checksum = re.search(HASH_SCHEMES[self.hash_scheme]["regex"], checksum).group(0)

            # Check that checksum in local file is correct, this is slow so don't make it default
            if self.recheck_checksum:
                recompute_checksum = compute_checksum(datafile, self.hash_scheme)
                if checksum != recompute_checksum:
                    logger.error(
                        f"Wrong checksum recorded for file: {datafile}. "
                        f"Recorded checksum: {checksum}, excepted checksum: {recompute_checksum}."
                    )
                    raise FileChecksumMismatchException

            rawdata_structure_dict[datafile.parent].append(
                FileDataObject(
                    file_name=datafile.name, file_path=str(datafile), file_checksum=checksum
                )
            )

        logger.info("... done with raw data files search.")

        # Return dictionary of dictionaries
        return rawdata_structure_dict


# Adapted from snappy.check_remote
class FileComparisonChecker:
    """Class with checker methods."""

    def __init__(
        self,
        local_files_dict,
        remote_files_dict,
        report_categories: list[typing.Literal["both", "remote-only", "local-only"]],
        filenames_only=False,
        irods_basepath=None,
        report_checksums=False,
    ):
        """Constructor.

        :param local_files_dict: Dictionary with local directories as keys and list of FileDataObject as values.
        :type local_files_dict: dict

        :param remote_files_dict: Dictionary with remote filenames as keys and list of IrodsDataObject as values.
        :type remote_files_dict: dict

        :param report_categories: List of category names to report.
        :type report_categories: list[typing.Literal["both", "remote-only", "local-only"]]

        :param filenames_only: Flag to indicate if checksums should not be used for comparison

        :param irods_basepath: assay basepath in irods that should be removed for reporting

        :param report_checksums: Flag to indicate if checksums should be included in report
        """
        self.local_files_dict = local_files_dict
        self.remote_files_dict = remote_files_dict
        self.report_categories = report_categories
        self.filenames_only = filenames_only
        self.irods_basepath = irods_basepath
        self.report_checksums = report_checksums

    def run(self):
        """Executes comparison of local and remote files"""
        # Run comparison
        in_both, local_only, remote_unmatched = self.compare_local_and_remote_files(
            self.local_files_dict, self.remote_files_dict, self.filenames_only, self.irods_basepath
        )

        # Same name in Sodar is only relevant if we only match by name
        if self.filenames_only:
            self.report_multiple_file_versions_in_sodar(self.remote_files_dict)

        self.report_findings(
            both_locations=in_both,
            only_local=local_only,
            only_remote=remote_unmatched,
            report_categories=self.report_categories,
            include_checksum=self.report_checksums,
        )

        # Return all okay
        return True

    @staticmethod
    def report_multiple_file_versions_in_sodar(remote_files_dict):
        return SnappyChecker.report_multiple_file_versions_in_sodar(remote_files_dict)

    # FIXME: reduce complexity
    @staticmethod
    def compare_local_and_remote_files(  # noqa: C901
        local_dict, remote_dict, filenames_only=False, irods_basepath=""
    ):
        """Compare locally and remotely available files.

        :param local_dict: Dictionary with local directories as keys and list of FileDataObject as values.
        :type local_dict: dict

        :param remote_dict: Dictionary with remote filenames as keys and list of IrodsDataObject as values.
        :type remote_dict: dict

        :param filenames_only: Flag to indicate if checksums should not be used for comparison

        :param irods_basepath: assay basepath in irods that should be removed for reporting
        :type irods_basepath: str

        :return: Returns tuple with three dictionaries: one for files that are found both locally and remotely;
        one for files only remotely; and one for files only locally. All use paths to file locations as keys and
        values are lists of FileDataObjects
        """

        def filedata_from_irodsdata(
            obj,
        ):  # Helper Function to convert iRodsDataObject (non-hashable) to FileDataObject, also making path relative
            p = Path(obj.path).parent
            if irods_basepath:
                try:
                    p = p.relative_to(irods_basepath)
                except ValueError:
                    pass  # wrong assay, skip
            return FileDataObject(obj.name, str(p), obj.checksum)

        # The dictionaries will contain double information on the file path (both as keys & in the objects)
        # For collecting info in itself sets would be easier, however grouping by folder makes it easier to
        # sort the files for reporting
        in_both = defaultdict(list)
        local_only = defaultdict(list)
        # Using a set (instead of dict for returning directly) is easier for checking which files get matched
        remote_unmatched = {
            filedata_from_irodsdata(f) for filename, files in remote_dict.items() for f in files
        }
        filenames_warnings = set()

        for directory, files in local_dict.items():
            for file in files:
                filename = file.file_name
                checksum = file.file_checksum
                # Check first if there is any matching remote file or if file is local only
                if filename not in remote_dict:
                    local_only[directory].append(file)
                    continue

                # Collect all remote files with same name
                remote_files = remote_dict[filename]

                # For filename based matching *all* files with the same name will be matched
                # If we match multiple files with the same name, they may not be the same, so warning is issued
                # TODO: maybe make this an error? optionally an error depending on flags?
                if filenames_only:
                    in_both[directory].append(file)
                    remote_unmatched -= {filedata_from_irodsdata(f) for f in remote_files}
                    if len(remote_files) > 1 and filename not in filenames_warnings:
                        filenames_warnings.add(filename)
                        logger.warning(
                            f"Local file ({filename}) matches {len(remote_files)} files in irods. "
                            f"Run without --filename-only to check individual files based on MD5 or SHA256 as well as name."
                        )
                    continue

                # From the file with matching names subselect those with same hash
                checksum_matches = {
                    filedata_from_irodsdata(f) for f in remote_files if f.checksum == checksum
                }
                if checksum_matches:
                    remote_unmatched -= checksum_matches
                    in_both[directory].append(file)
                else:
                    local_only[directory].append(file)
                # Multiple files with the same checskum aren't a critical issue - an info/warning is enough here
                if len(checksum_matches) > 1:
                    logger.info(
                        f"Local file ({filename}) matches {len(checksum_matches)} files with the same checksum in irods."
                    )

        # Convert set of unmatched files into the same dict structure as the others
        remote_only = defaultdict(list)
        for file in remote_unmatched:
            remote_only[file.file_path].append(file)

        return in_both, local_only, remote_only

    @staticmethod
    def report_findings(
        both_locations, only_local, only_remote, report_categories, include_checksum=False
    ):
        """Report findings

        :param both_locations: Dict for files found both locally and in remote directory.
        Keys: local directories, values: list of FileDataObject
        :type both_locations: dict

        :param only_local: Dict for files found only in the local directory.
        Keys: local directories, values: list of FileDataObject
        :type only_local: dict

        :param only_remote: Dict with files found only in the remote directory.
        Keys: irods paths, values: list of FileDataObject
        :type only_remote: dict

        :param report_categories: List of category names to report.
        :type report_categories: list[typing.Literal["both", "remote-only", "local-only"]]

        :param include_checksum: Flag to indicate if checksums should be included in reports
        """

        # Convert entries to text
        def make_file_block(folder, files):
            files_str = "\n".join(
                "    "
                + f.file_name
                + ("" if not include_checksum else "  (" + f.file_checksum[:8] + ")")
                for f in sorted(files, key=lambda o: o.file_name)
            )
            return str(folder) + ":\n" + files_str

        in_both_str = "\n".join(
            sorted((make_file_block(folder, files) for folder, files in both_locations.items()))
        )
        local_only_str = "\n".join(
            sorted((make_file_block(folder, files) for folder, files in only_local.items()))
        )
        remote_only_str = "\n".join(
            sorted((make_file_block(folder, files) for folder, files in only_remote.items()))
        )

        dashed_line = "-" * 25

        # Write results to stdout
        if len(both_locations) > 0 and "both" in report_categories:
            print(f"Files found BOTH locally and remotely:\n{in_both_str}")
        elif "both" in report_categories:
            print("No file was found both locally and remotely.")
        if "both" in report_categories and "local-only" in report_categories:
            print(dashed_line)
        if len(only_local) > 0 and "local-only" in report_categories:
            print(f"Files found ONLY LOCALLY:\n{local_only_str}")
        elif "local-only" in report_categories:
            print("No file found only locally.")
        if "remote-only" in report_categories and report_categories != ["remote-only"]:
            print(dashed_line)
        if len(only_remote) > 0 and "remote-only" in report_categories:
            print(f"Files found ONLY REMOTELY:\n{remote_only_str}")
        elif "remote-only" in report_categories:
            print("No file found only remotely.")


# Adapted from snappy check_remote
class SodarCheckRemoteCommand:
    """Implementation of the ``check-remote`` command."""

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
            "-p",
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path in which local files with checksums should be identified. Default: CWD"
            ),
        )
        parser.add_argument(
            "--file-selection-regex",
            default=None,
            help="(Regex) pattern to select files for comparison. I.e.: 'fastq.gz$'. Default: None (all files used)",
        )
        parser.add_argument(
            "--filename-only",
            default=False,
            action="store_true",
            help="Flag to indicate whether file comparison between local and remote files "
            "should only use file names and ignore checksum values.",
        )
        parser.add_argument(
            "--recheck-checksum",
            default=False,
            action="store_true",
            help="Flag to double check that checksums stored in local files do actually match their corresponding files",
        )
        parser.add_argument(
            "--report-checksums",
            default=False,
            action="store_true",
            help="Flag to indicate if checksums should be included in file report",
        )
        parser.add_argument(
            "--report-categories",
            choices=["remote-only", "local-only", "both"],
            nargs="+",
            default=["remote-only", "local-only", "both"],
            help="Flag to select categories of checked files to report (any of: remote-only, local-only, both). Default: all reported.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    @staticmethod
    def check_args(args):
        """Called for checking arguments."""
        res = 0

        # Validate base path
        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path {} does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar check-remote")
        print_args(self.args)

        # Find all remote files (iRODS)
        irodscollector = RetrieveSodarCollection(self.args)
        hash_scheme = irodscollector.irods_hash_scheme
        remote_files_dict = irodscollector.perform()
        assay_path = irodscollector.get_assay_irods_path()

        # Filter irods file list by file-name regex if given
        if self.args.file_selection_regex:
            pattern = re.compile(self.args.file_selection_regex)
            remote_files_dict = {
                k: [v for v in vals if re.search(pattern, str(v.name))]
                for k, vals in remote_files_dict.items()
            }

        # Find all local files with checksum, includes regex filter
        local_files_dict = FindLocalChecksumFiles(
            base_path=self.args.base_path,
            hash_scheme=hash_scheme,
            recheck_checksum=self.args.recheck_checksum,
            regex_pattern=self.args.file_selection_regex,
        ).run()

        # Run checks
        FileComparisonChecker(
            remote_files_dict=remote_files_dict,
            local_files_dict=local_files_dict,
            report_categories=self.args.report_categories,
            filenames_only=self.args.filename_only,
            irods_basepath=assay_path,
            report_checksums=self.args.report_checksums,
        ).run()

        logger.info("All done.")
        return 0


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-remote``."""
    return SodarCheckRemoteCommand.setup_argparse(parser)
