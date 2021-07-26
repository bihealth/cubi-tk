"""``cubi-tk snappy check-remote``: check that files are present in remote SODAR/iRODS.

Only uses local information for checking that the linked-in RAW data files are correct in terms
of the MD5 sum.  Otherwise, just checks for presence of files (for now), the rationale being that

"""
import argparse
from collections import defaultdict
import os
from pathlib import Path
import shlex
import typing
from subprocess import SubprocessError, check_output

from biomedsheets import shortcuts
from logzero import logger
from sodar_cli import api

from .common import get_biomedsheet_path, load_sheet_tsv
from ..common import load_toml_config


class FindFilesCommon:
    """Class contains common methods used to find files."""

    def __init__(self, sheet):
        """Constructor.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.shortcuts.GermlineCaseSheet
        """
        self.sheet = sheet

    def parse_sample_sheet(self):
        """Parse sample sheet.

        :return: Returns list of library names - used to define directory names though out the pipeline.
        """
        # Initialise variables
        library_names = []
        # Iterate over sample sheet
        for pedigree in self.sheet.cohort.pedigrees:
            for donor in pedigree.donors:
                library_names.append(donor.dna_ngs_library.name)
        # Return list of identifiers
        return library_names

    @staticmethod
    def iter_dirs(path):
        """Directory iterator.

        From Stack Overflow: https://stackoverflow.com/questions/57910227
        """
        for file_or_directory in path.rglob("*"):
            if file_or_directory.is_dir():
                yield file_or_directory


class FindLocalRawdataFiles(FindFilesCommon):
    """Class finds and lists local raw data files associated with samples."""

    def __init__(self, sheet, base_path):
        """Constructor.

        :param base_path: Base project path.
        :type base_path: str
        """
        super().__init__(sheet)
        self.inlink_dir_path = Path(base_path) / "ngs_mapping" / "work" / "input_links"

    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries: key: library name (e.g., 'P001-N1-DNA1-WES1'); value: dictionary
        with list of files (values) per directory (key).
        """
        logger.info("Starting raw data files search ...")

        # Initialise variables
        rawdata_structure_dict = defaultdict(dict)

        # Validate input
        if not self.inlink_dir_path.exists():
            logger.error(
                "Path to directory linked to raw data does not exist. Expected: {path}".format(
                    path=self.inlink_dir_path
                )
            )
            return None

        # Get all libraries
        library_names = self.parse_sample_sheet()

        # Get directory structure
        for i_directory in self.iter_dirs(self.inlink_dir_path):
            # Find library name if any
            library_name = None
            for lib in library_names:
                if lib in str(i_directory.resolve()):
                    library_name = lib
                    break
            if not library_name:
                continue
            # Update dictionary
            i_file_list = [
                scanned.name for scanned in os.scandir(i_directory) if scanned.is_file()
            ]  # filter files
            i_file_list = [
                file for file in i_file_list if not file.startswith(".")
            ]  # filter for example '.done'
            if len(i_file_list) > 0:
                library_local_files_dict = {str(i_directory): i_file_list}
                rawdata_structure_dict[library_name].update(library_local_files_dict)

        logger.info("... done with raw data files search.")

        # Return dictionary of dictionaries
        return rawdata_structure_dict


class FindLocalFiles(FindFilesCommon):
    """Class finds and lists local files associated with samples."""

    def __init__(self, sheet, base_path, step_list=None):
        """Constructor.

        :param base_path: Base project path.
        :type base_path: str

        :param step_list: List of steps being analyzed, e.g.: ['ngs_mapping', 'variant_calling'].
        :type step_list: list
        """
        super().__init__(sheet)
        self.base_path = base_path
        if step_list is None or len(step_list) == 0:
            raise ValueError(
                "Step list cannot be empty. Expected input: ['ngs_mapping', 'variant_calling']"
            )
        self.step_list = step_list

    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries: key: step name (e.g., 'variant_calling'); value: dictionary
        with file structure per library/sample.
        """
        logger.info("Starting local files search ...")

        # Initialise variables
        canonical_paths = {}
        step_to_file_structure_dict = defaultdict(lambda: defaultdict(dict))

        # Get all libraries
        library_names = self.parse_sample_sheet()

        # Define canonical paths
        path = Path(self.base_path)
        for step in self.step_list:
            tmp_path = path / step / "output"
            # Send only warning if it doesn't exist: error will be handle by
            # respective step checker classes, so we can have at least a partial run.
            if tmp_path.exists():
                canonical_paths.update({step: tmp_path})
            else:
                logger.warn(
                    "Canonical path for step '{step}' does not exist. Expected: {path}".format(
                        step=step, path=str(tmp_path)
                    )
                )

        # Iterate over all directories
        for step_check in canonical_paths:
            c_path = canonical_paths.get(step_check)
            for i_directory in self.iter_dirs(c_path):
                # Find library name if any
                library_name = None
                for lib in library_names:
                    if lib in str(i_directory.resolve()):
                        library_name = lib
                        break
                if not library_name:
                    continue
                # Update dictionary
                i_file_list = [
                    scanned.name for scanned in os.scandir(i_directory) if scanned.is_file()
                ]
                if len(i_file_list) > 0:
                    library_local_files_dict = {str(i_directory): i_file_list}
                    step_to_file_structure_dict[step_check][library_name].update(
                        library_local_files_dict
                    )

        logger.info("... done with local files search.")

        # Return dictionary of dictionaries
        return step_to_file_structure_dict


class FindRemoteFiles(FindFilesCommon):
    """Class finds and lists remote files associated with samples."""

    def __init__(self, sheet, sodar_url, sodar_api_token, project_uuid):
        """Constructor.

        :param sodar_url: SODAR url.
        :type sodar_url: str

        :param sodar_api_token: SODAR API token.
        :type sodar_api_token: str

        :param project_uuid: Project UUID.
        :type project_uuid: str
        """
        super().__init__(sheet)
        self.sodar_url = sodar_url
        self.sodar_api_token = sodar_api_token
        self.project_uuid = project_uuid

    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries: key: library name (e.g., 'P001-N1-DNA1-WES1'); value: dictionary
        with list of files (values) per remote directory (key).
        """
        logger.info("Starting remote files search ...")
        # Initialise variables
        library_remote_files_dict = {}

        # Get assay irods path
        assay_path = self.get_assay_irods_path()

        # Get all libraries - iterate over to get remote files
        library_names = self.parse_sample_sheet()
        for library in library_names:
            ils_stdout = self.find_remote_files(library_name=library, irods_path=assay_path)
            parsed_ils = self.parse_ils_stdout(ils_bytes=ils_stdout)
            library_remote_files_dict[library] = parsed_ils

        logger.info("... done with remote files search.")

        # Return dict of dicts
        return library_remote_files_dict

    def get_assay_irods_path(self):
        """Get Assay iRODS path.

        :return: Returns Assay iRODS path - extracted via SODAR API.
        """
        investigation = api.samplesheet.retrieve(
            sodar_url=self.sodar_url,
            sodar_api_token=self.sodar_api_token,
            project_uuid=self.project_uuid,
        )
        for study in investigation.studies.values():
            for assay_uuid in study.assays.keys():
                # TODO: Naive assumption that there is only one assay per study - review it.
                assay = study.assays[assay_uuid]
                return assay.irods_path
        return None

    @staticmethod
    def parse_ils_stdout(ils_bytes):
        """Parses `ils` call stdout.

        :param ils_bytes: ils command call stdout.
        :type ils_bytes: bytes

        :return: Returns dictionary with remote files and directories as found in `ils` call. Key: remote directory
        path; Value: list of files in remote directory.
        """
        # Initialise variables
        dir_to_files_dict = defaultdict(list)  # key: iRODS directory path; value: list of files
        # Convert bytes to str
        ils_str = str(ils_bytes.decode())
        lines = ils_str.splitlines()
        # Remove directories with no files, just subdirectories.
        # Example: '  C- /sodarZone/projects/17/99999999-aaaa-bbbb-cccc-999999999999/sample_data/...'
        lines = [line for line in lines if not line.startswith("  C- ")]
        # Populate dictionary with iRODS content
        current_directory = None
        for line in lines:
            if line.startswith("/"):
                current_directory = line.replace(":", "")
                continue
            dir_to_files_dict[current_directory].append(line.replace(" ", ""))
        return dir_to_files_dict

    @staticmethod
    def find_remote_files(library_name, irods_path):
        """Find files in iRODS.

        :param library_name: Sample's library name. Example: 'P001-N1-DNA1-WES1'.
        :type library_name: str

        :param irods_path: Path to Assay in iRODS.
        :type irods_path: str

        :return: Returns `ils` call stdout.
        """
        cmd = "ils", "-r", "%s/%s" % (irods_path, library_name)
        try:
            cmd_str = " ".join(map(shlex.quote, cmd))
            logger.info("Executing %s", cmd_str)
            return check_output(cmd)
        except SubprocessError as e:  # pragma: nocover
            logger.error("Problem executing `ils`: %s", e)


class Checker:
    """Class with common checker methods."""

    def __init__(self, local_files_dict, remote_files_dict, check_md5=False):
        """ Constructor.

        :param local_files_dict: Dictionary with local files and directories structure for all libraries in sample
        sheet.
        :type local_files_dict: dict

        :param remote_files_dict: Dictionary with remote files and directories structure for all libraries in sample
        sheet.
        :type remote_files_dict: dict

        :param check_md5: Flag to indicate if local MD5 files should be compared with
        """
        self.local_files_dict = local_files_dict
        self.remote_files_dict = remote_files_dict
        self.check_md5 = check_md5

    def coordinate_run(self, check_name):
        """Coordinates the execution of methods necessary to check step files.

        :param check_name: Step name being checked.
        :type check_name: str
        """
        # Initialise variables
        in_both_set = set()
        remote_only_set = set()
        local_only_set = set()

        # Validate input
        if not self.local_files_dict:
            logger.error("Dictionary is empty for step '{step}'.".format(step=check_name))
            return False

        # Iterate over libraries
        for library_name in self.remote_files_dict:
            # Restrict dictionary to directories associated with step
            subset_remote_files_dict = {}
            for key, value in self.remote_files_dict.get(library_name).items():
                if check_name in key:
                    subset_remote_files_dict[key] = value
            # Find local files
            subset_local_files_dict = self.local_files_dict.get(library_name)
            # Compare dictionaries
            i_both, i_remote, i_local = self.compare_local_and_remote_files(
                local_dict=subset_local_files_dict,
                remote_dict=subset_remote_files_dict,
                check_name=check_name,
                library_name=library_name,
            )
            in_both_set.update(i_both)
            remote_only_set.update(i_remote)
            local_only_set.update(i_local)

        # Report
        self.report_findings(
            both_locations=in_both_set, only_local=local_only_set, only_remote=remote_only_set
        )

        # Return all okay
        return True

    @staticmethod
    def compare_local_and_remote_files(local_dict, remote_dict, check_name, library_name):
        """Compare locally and remotely available files.

        :param local_dict: Dictionary with local file structure. Key: directory path; Value: list of file names. Paths
        in dictionary are expected to have an extra `output` subdirectory.
        :type local_dict: dict

        :param remote_dict: Dictionary with remote file structure. Key: remote directory path; Value: list of file
        names.
        :type remote_dict: dict

        :param check_name: Check name, e.g.: 'ngs_mapping' or 'variant_calling'.
        :type check_name: str

        :param library_name: Library name, e.g.: 'P001-N1-DNA1-WES1'.
        :type library_name: str

        :return: Returns tuple with three sets: one for files that are found both locally and remotely; one for files
        only found remotely; and, one for files only found locally.
        """
        # Initialise variables
        all_remote_files_set = set()
        all_local_files_set = set()
        in_both_set = set()
        only_remote_set = set()
        only_local_set = set()
        file_to_remote_path_dict = defaultdict(list)
        file_to_local_path_dict = defaultdict(list)

        # Get all files remote
        for remote in remote_dict:
            for file in remote_dict.get(remote):
                file_to_remote_path_dict[file].append(remote)
                all_remote_files_set.add(file)

        # Get all files local
        for local in local_dict:
            for file in local_dict.get(local):
                file_to_local_path_dict[file].append(local)
                all_local_files_set.add(file)

        # Present in both - stores only local path
        for file in all_remote_files_set.intersection(all_local_files_set):
            in_both_set.update([local + "/" + file for local in file_to_local_path_dict.get(file)])

        # Only remote
        for file in all_remote_files_set - all_local_files_set:
            only_remote_set.update(
                [remote + "/" + file for remote in file_to_remote_path_dict.get(file)]
            )

        # Only local
        for file in all_local_files_set - all_remote_files_set:
            only_local_set.update(
                [local + "/" + file for local in file_to_local_path_dict.get(file)]
            )

        # Return
        return in_both_set, only_remote_set, only_local_set

    @staticmethod
    def report_findings(both_locations, only_remote, only_local):
        """Report findings

        :param both_locations: Set with files found both locally and in remote directory.
        :type both_locations: set

        :param only_remote: Set with files found only in the remote directory.
        :type only_remote: set

        :param only_local: Set with files found only in the local directory.
        :type only_local: set
        """
        # Convert entries to text
        in_both_str = "\n".join(both_locations)
        remote_only_str = "\n".join(only_remote)
        local_only_str = "\n".join(only_local)

        # Log
        if len(both_locations) > 0:
            logger.info("Files found BOTH locally and remotely:\n{files}".format(files=in_both_str))
        else:
            logger.warn("No file was found both locally and remotely.")
        if len(only_remote) > 0:
            logger.warn("Files found ONLY REMOTELY:\n{files}".format(files=remote_only_str))
        else:
            logger.info("No file found only remotely.")
        if len(only_local) > 0:
            logger.warn("Files found ONLY LOCALLY:\n{files}".format(files=local_only_str))
        else:
            logger.warn("No file found only locally.")


class RawDataChecker(Checker):
    """Check for raw data being present and equal as in local ``ngs_mapping`` directory."""

    #: Step name being checked.
    check_name = "raw_data"

    def __init__(self, sheet, base_path, local_files_dict, remote_files_dict, check_md5):
        """ Constructor.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.shortcuts.GermlineCaseSheet

        :param base_path: Base project path.
        :type base_path: str
        """
        super().__init__(local_files_dict, remote_files_dict, check_md5)
        self.sheet = sheet
        self.base_path = base_path

    def run(self):
        """Executes checks for Raw Data files."""
        logger.info("Starting raw data checks ...")
        # Initialise variable
        out_flag = False
        # Get local raw data files
        self.local_files_dict = FindLocalRawdataFiles(
            sheet=self.sheet, base_path=self.base_path
        ).run()
        if self.local_files_dict:
            # Compare local and remote
            out_flag = self.coordinate_run(check_name=self.check_name)
        logger.info("... done with raw data checks.")
        return out_flag


class NgsMappingChecker(Checker):
    """Check for mapping results being present without checking content."""

    #: Step name being checked.
    check_name = "ngs_mapping"

    def __init__(self, *args, **kwargs):
        """ Constructor."""
        super().__init__(*args, **kwargs)

    def run(self):
        """Executes checks for NGS mapping files."""
        logger.info("Starting ngs_mapping checks ...")
        out_flag = self.coordinate_run(check_name=self.check_name)
        logger.info("... done with ngs_mapping checks.")
        return out_flag


class VariantCallingChecker(Checker):
    """Check for variant calling results being present without checking content"""

    #: Step name being checked.
    check_name = "variant_calling"

    def __init__(self, *args, **kwargs):
        """ Constructor."""
        super().__init__(*args, **kwargs)

    def run(self):
        """Executes checks for Variant Calling files."""
        logger.info("Starting variant_calling checks ...")
        out_flag = self.coordinate_run(check_name=self.check_name)
        logger.info("... done with variant_calling checks.")
        return out_flag


class SnappyCheckRemoteCommand:
    """Implementation of the ``check-remote`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        # Find biomedsheet file
        self.biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=args.project_uuid
        )
        #: Raw sample sheet.
        self.sheet = load_sheet_tsv(self.biomedsheet_tsv, args.tsv_shortcut)
        #: Shortcut sample sheet.
        self.shortcut_sheet = shortcuts.GermlineCaseSheet(self.sheet)

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-remote`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
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
            "--tsv-shortcut",
            default="germline",
            choices=("germline", "cancer"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path of project (contains 'ngs_mapping/' etc.), spiders up from biomedsheet_tsv and falls "
                "back to current working directory by default."
            ),
        )
        parser.add_argument("project_uuid", type=str, help="UUID from project to check.")

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

        logger.info("Starting cubi-tk snappy check-remote")
        logger.info("  args: %s", self.args)

        # Find all remote files (iRODS)
        library_remote_files_dict = FindRemoteFiles(
            self.shortcut_sheet,
            self.args.sodar_url,
            self.args.sodar_api_token,
            self.args.project_uuid,
        ).run()

        # Find all local files (canonical paths)
        library_local_files_dict = FindLocalFiles(
            sheet=self.shortcut_sheet,
            base_path=self.args.base_path,
            step_list=["ngs_mapping", "variant_calling"],
        ).run()

        # Run checks
        results = [
            RawDataChecker(
                sheet=self.shortcut_sheet,
                base_path=self.args.base_path,
                remote_files_dict=library_remote_files_dict,
                local_files_dict={},  # special case: dict correctly defined inside class
                check_md5=False,  # special case: it will never check MD5 for raw data
            ).run(),
            NgsMappingChecker(
                remote_files_dict=library_remote_files_dict,
                local_files_dict=library_local_files_dict.get("ngs_mapping"),
            ).run(),
            VariantCallingChecker(
                remote_files_dict=library_remote_files_dict,
                local_files_dict=library_local_files_dict.get("variant_calling"),
            ).run(),
        ]
        if all(results):
            logger.info("All done.")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-remote``."""
    return SnappyCheckRemoteCommand.setup_argparse(parser)
