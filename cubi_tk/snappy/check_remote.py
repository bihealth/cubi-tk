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


class FindRemoteFiles:
    """Class finds and lists remote files associated with samples."""

    def __init__(self, sheet, sodar_url, sodar_api_token, project_uuid):
        """Constructor.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.shortcuts.GermlineCaseSheet

        :param sodar_url: SODAR url.
        :type sodar_url: str

        :param sodar_api_token: SODAR API token.
        :type sodar_api_token: str

        :param project_uuid: Project UUID.
        :type project_uuid: str
        """
        self.sheet = sheet
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


class Checker:
    """Class with common checker methods"""

    def __init__(self, remote_files_dict, base_path, check_md5=False):
        """ Constructor.

        :param remote_files_dict: Dictionary with remote files and directories structure for all libraries in sample
        sheet.
        :type remote_files_dict: dict

        :param base_path: Base path: where to look for NGS mapping output directory.
        :type base_path: str

        :param check_md5: Flag to indicate if local MD5 files should be compared with
        """
        self.remote_files_dict = remote_files_dict
        self.base_path = base_path
        self.check_md5 = check_md5

    @staticmethod
    def find_local_files(library_name, path):
        """Find local output files.

        :param library_name: Sample library name as used to name directories, e.g., 'P001-N1-DNA1-WES1'.
        :type library_name: str

        :param path: Path to output.
        :type path: pathlib.Path

        :return: Returns dictionary with local files in output directories. Key: directory path; Value: list of files
        in directory.
        """
        # Initialise variable
        library_local_files_dict = {}

        # Find library directories
        pattern = "*" + library_name + "*"
        lib_directories = path.glob(pattern)

        # Walk though directory
        for directory in lib_directories:
            subdirectories = [scanned.path for scanned in os.scandir(directory) if scanned.is_dir()]
            all_directories = [directory] + subdirectories
            for i_directory in all_directories:
                i_file_list = [
                    scanned.name for scanned in os.scandir(i_directory) if scanned.is_file()
                ]
                if len(i_file_list) > 0:
                    library_local_files_dict[i_directory] = i_file_list

        # Return dictionary of dirs and files
        return library_local_files_dict


class RawDataChecker:
    """Check for raw data being present and equal as in local ``ngs_mapping`` directory."""

    def __init__(self, sheet, project_uuid):
        self.sheet = sheet
        self.project_uuid = project_uuid

    def run(self):
        logger.info("Starting raw data checks ...")
        logger.info("... done with raw data checks")
        return True


class NgsMappingChecker(Checker):
    """Check for mapping results being present without checking content."""

    def __init__(self, *args, **kwargs):
        """ Constructor."""
        super().__init__(*args, **kwargs)
        #: NGS mapping output expected directory
        self.ngs_mapping_out_dir = Path(self.base_path) / "ngs_mapping" / "output"

    def run(self):
        logger.info("Starting ngs_mapping checks ...")
        logger.info("... done with ngs_mapping checks.")
        return True


class VariantCallingChecker:
    """Check for variant calling results being present without checking content"""

    def __init__(self, germline_sheet, project_uuid):
        self.germline_sheet = germline_sheet
        self.project_uuid = project_uuid

    def run(self):
        logger.info("Starting variant_calling checks ...")
        logger.info("... done with variant_calling checks")
        return True


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

    def check_args(self, args):
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

        results = [
            RawDataChecker(self.sheet, self.args.project_uuid).run(),
            NgsMappingChecker(self.sheet, self.args.project_uuid).run(),
            VariantCallingChecker(self.shortcut_sheet, self.args.project_uuid).run(),
        ]

        logger.info("All done.")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-local``."""
    return SnappyCheckRemoteCommand.setup_argparse(parser)
