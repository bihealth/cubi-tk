"""``cubi-tk sodar check-remote``: check that files are present in remote SODAR/iRODS.

Only uses local information for checking that the linked-in RAW data files are correct in terms
of the MD5 sum.

"""
import argparse
from collections import defaultdict
import os
from pathlib import Path
from types import SimpleNamespace
import typing
import attr

from logzero import logger

from ..common import load_toml_config, compute_md5_checksum
from ..irods.check import DEFAULT_HASH_SCHEME
from ..snappy.retrieve_irods_collection import RetrieveIrodsCollection
from ..snappy.check_remote import Checker as SnappyChecker

# Notes:
# - will not find files that have correct md5sum but different name in irods


# Adapted from snappy.retrieve_irods_collection
@attr.s(frozen=True, auto_attribs=True)
class FileDataObject:
    """File data object - simple container to keep track of files information and allow comparison"""

    file_name: str
    file_path: str
    file_md5sum: str


class FindLocalMD5Files:
    """Class contains methods to find local files with associated md5 sums"""

    def __init__(self, base_path, recheck_md5):
        """Constructor: init vars"""

        self.searchpath = Path(base_path)

        self.recheck_md5 = recheck_md5

        if self.recheck_md5 and DEFAULT_HASH_SCHEME != "MD5":
            logger.warning(
                f"Recalculation of HASH other than MD5 not implemented yet! No hashes will be re-checked."
            )

    # Adapted from snappy check remote
    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries:
        key: directory path ; value: list of FileDataObject (one per file in that path)
        """
        logger.info("Starting raw data files search ...")

        # Initialise variables
        rawdata_structure_dict = defaultdict(list)

        # Validate input
        if not self.searchpath.exists():
            logger.error(
                f"Path to directory to raw data does not exist. Expected: {self.searchpath}"
            )
            return None

        # Find all md5 files
        # Todo/Maybe: search based on DEFAULT_HASH_SCHEME.lower()
        # -> needs some other changes, only really useful once we start using something other than md5
        md5_files = self.searchpath.rglob("*.md5")

        # Check that corresponding files exist
        for md5file in md5_files:

            datafile = md5file.with_suffix("")
            if not datafile.exists():
                logger.warning(
                    f"Orphaned local md5 file encountered: {md5file}. Ignoring expected: {datafile}"
                )
                continue

            with open(md5file, "r", encoding="utf8") as f:
                md5sum = f.readline()
                # Expected format example:
                # `459db8f7cb0d3a23a38fdc98286a9a9b  out.vcf.gz`
                md5sum = md5sum.split(" ")[0]

            # Check that md5 sum in local file is correct, this is slow so don't make it default
            if self.recheck_md5 and DEFAULT_HASH_SCHEME == "MD5":
                recompute_md5 = compute_md5_checksum(datafile)
                # TODO: this should probably be more than a warning?
                if md5sum != recompute_md5:
                    logger.warning(
                        f"Wrong md5 sum recorded for file: {datafile}. "
                        f"Recorded md5: {md5sum}, excepted md5: {recompute_md5}. Ignoring this file"
                    )
                    continue

            rawdata_structure_dict[datafile.parent].append(
                FileDataObject(
                    file_name=datafile.name,
                    file_path=str(datafile),
                    file_md5sum=md5sum,
                )
            )

        logger.info("... done with raw data files search.")

        # Return dictionary of dictionaries
        return rawdata_structure_dict


# Adapted from snappy.check_remote
class FileComparisonChecker:
    """Class with checker methods."""

    def __init__(self, local_files_dict, remote_files_dict, filenames_only=False, irods_basepath = None):
        """Constructor.

        :param local_files_dict: Dictionary with local directories as keys and list of FileDataObject as values.
        :type local_files_dict: dict

        :param remote_files_dict: Dictionary with remote filenames as keys and list of IrodsDataObject as values.
        :type remote_files_dict: dict

        :param filenames_only: Flag to indicate if md5 sums should not be used for comparison

        :param irods_basepath: assay basepath in irods that should be removed for reporting
        """
        self.local_files_dict = local_files_dict
        self.remote_files_dict = remote_files_dict
        self.filenames_only = filenames_only
        self.irods_basepath = irods_basepath

    def run(self):
        """Executes comparison of local and remote files"""
        # Run comparison
        in_both, local_only, remote_unmatched = self.compare_local_and_remote_files(
            self.local_files_dict, self.remote_files_dict, self.filenames_only, self.irods_basepath
        )

        # Same name in Sodar is only relevant if we only match by name
        if self.filenames_only:
            self.report_multiple_file_versions_in_sodar(remote_dict=self.remote_files_dict)

        self.report_findings(
            both_locations=in_both, only_local=local_only, only_remote=remote_unmatched
        )

        # Return all okay
        return True

    @staticmethod
    def compare_local_and_remote_files(local_dict, remote_dict, filenames_only=False, irods_basepath=""):
        """Compare locally and remotely available files.

        :param local_dict: Dictionary with local directories as keys and list of FileDataObject as values.
        :type local_dict: dict

        :param remote_dict: Dictionary with remote filenames as keys and list of IrodsDataObject as values.
        :type remote_dict: dict

        :param filenames_only: Flag to indicate if md5 sums should not be used for comparison

        :param irods_basepath: assay basepath in irods that should be removed for reporting
        :type irods_basepath: str

        :return: Returns tuple with three dictionaries: one for files that are found both locally and remotely;
        one for files only remotely; and one for files only locally. All use paths to file locations as keys and
        values are lists of FileDataObjects
        """

        def filedata_from_irodsdata(obj):
            # Helper Function to convert IrodsDataObject (non-hashable) to FileDataObject, also making path relative
            p = Path(obj.irods_path).parent
            p = p.relative_to(irods_basepath) if irods_basepath else p
            return FileDataObject(obj.file_name, str(p), obj.file_md5sum)

        # The dictionaries will contain double information on the file path (both as keys & in the objects)
        # For collecting info in itself sets would be easier, however grouping by folder makes it easier to
        # sort the files for reporting
        in_both = defaultdict(list)
        local_only = defaultdict(list)
        # Using a set instead of dict for returning directly here is easier for checking which files get matched
        remote_unmatched = {
            filedata_from_irodsdata(f)
            for filename, files in remote_dict.items()
            for f in files
        }
        filenames_warnings = set()

        for directory, files in local_dict.items():
            for file in files:
                filename = file.file_name
                md5 = file.file_md5sum
                if filename in remote_dict:
                    remote_files = remote_dict[filename]
                    if filenames_only:
                        # All files with the same name will be matched
                        in_both[directory].append(file)
                        remote_unmatched -= {filedata_from_irodsdata(f) for f in remote_files}
                        # If we match multiple files with the same name, they are likely not the same file, so
                        # give *at least* a warning
                        # TODO: maybe make this an error? optionally an error depending on flags?
                        if len(remote_files) > 1 and filename not in filenames_warnings:
                            filenames_warnings.add(filename)
                            logger.warning(
                                f"Local file ({filename}) matches {len(remote_files)} files in irods. "
                                f"Run without --filename-only to check individual files based on MD5 as well as name."
                            )
                    else:
                        md5_matches = {
                            filedata_from_irodsdata(f) for f in remote_files if f.file_md5sum == md5
                        }
                        if md5_matches:
                            remote_unmatched -= md5_matches
                            in_both[directory].append(file)
                        else:
                            local_only[directory].append(file)
                        # Multiple files with the same md5 aren't a critical issue - an info/warning is enough here
                        if len(md5_matches) > 1:
                            logger.info(
                                f"Local file ({filename}) matches {len(md5_matches)} files with the same md5sum in irods."
                            )
                else:
                    local_only[directory].append(file)

        # Convert set of unmatched files into the same dict structure as the others
        remote_only = defaultdict(list)
        for file in remote_unmatched:
            remote_only[file.file_path].append(file)

        return in_both, local_only, remote_only

    @staticmethod
    def report_multiple_file_versions_in_sodar(remote_dict):
        SnappyChecker.report_multiple_file_versions_in_sodar(remote_dict)

    @staticmethod
    def report_findings(both_locations, only_local, only_remote, include_md5=True):
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

        :param include_md5: Flag to indicate if md5 sums should be included in reports
        """
        # Convert entries to text
        def make_file_block(folder, files):
            files_str = "\n".join("    " + f.file_name + ("" if not include_md5 else "  (" + f.file_md5sum[:8] + ")")
                                  for f in sorted(files, key=lambda o: o.file_name))
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

        # Log
        if len(both_locations) > 0:
            logger.info(f"Files found BOTH locally and remotely:\n{in_both_str}\n{dashed_line}")
        else:
            logger.warn(f"No file was found both locally and remotely.\n{dashed_line}")
        if len(only_local) > 0:
            logger.warn(f"Files found ONLY LOCALLY:\n{local_only_str}\n{dashed_line}")
        else:
            logger.info(f"No file found only locally.\n{dashed_line}")
        if len(only_remote) > 0:
            logger.info(f"Files found ONLY REMOTELY:\n{remote_only_str}")
        else:
            logger.info("No file found only remotely.")


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
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        parser.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )
        parser.add_argument(
            "-p",
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path in which local files with md5 sums should be identified. Default: CWD"
            ),
        )
        parser.add_argument(
            "--filename-only",
            default=False,
            action="store_true",
            help="Flag to indicate whether file comparison between local and remote files "
            "should only use file names and ignore md5 values.",
        )
        parser.add_argument(
            "--recheck-md5",
            default=False,
            action="store_true",
            help="Flag to double check that md5 sums stored in local files do actually match their corresponding files",
        )

        parser.add_argument(
            "--assay-uuid",
            default=None,
            type=str,
            help="UUID from Assay to check. Used to specify target while dealing with multi-assay projects.",
        )
        parser.add_argument("project_uuid", type=str, help="UUID from Project to check.")

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

        # If SODAR info not provided, fetch from user's toml file
        toml_config = load_toml_config(args)
        args.sodar_url = args.sodar_url or toml_config.get("global", {}).get("sodar_server_url")
        args.sodar_api_token = args.sodar_api_token or toml_config.get("global", {}).get(
            "sodar_api_token"
        )

        # Validate base path
        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar check-remote")
        logger.info("  args: %s", self.args)

        # Find all remote files (iRODS)
        pseudo_args = SimpleNamespace(hash_scheme=DEFAULT_HASH_SCHEME)
        IrodsCollector = RetrieveIrodsCollection(
            pseudo_args,
            self.args.sodar_url,
            self.args.sodar_api_token,
            self.args.assay_uuid,
            self.args.project_uuid,
        )

        remote_files_dict = IrodsCollector.perform()
        assay_path = IrodsCollector.get_assay_irods_path(self.args.assay_uuid)

        # Find all local files with md5 sum
        local_files_dict = FindLocalMD5Files(
            base_path=self.args.base_path, recheck_md5=self.args.recheck_md5
        ).run()

        # Run checks
        results = FileComparisonChecker(
            remote_files_dict=remote_files_dict,
            local_files_dict=local_files_dict,
            filenames_only=self.args.filename_only,
            irods_basepath = assay_path
        ).run()

        if results:
            logger.info("All done.")
        return results


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-remote``."""
    return SodarCheckRemoteCommand.setup_argparse(parser)
