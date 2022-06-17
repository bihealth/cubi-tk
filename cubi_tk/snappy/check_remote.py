"""``cubi-tk snappy check-remote``: check that files are present in remote SODAR/iRODS.

Only uses local information for checking that the linked-in RAW data files are correct in terms
of the MD5 sum.  Otherwise, just checks for presence of files (for now), the rationale being that

"""
import argparse
from collections import defaultdict
import os
from pathlib import Path
import re
from types import SimpleNamespace
import typing

from biomedsheets import shortcuts
from logzero import logger
from sodar_cli import api

from .common import get_biomedsheet_path, load_sheet_tsv
from ..common import load_toml_config
from ..irods.check import IrodsCheckCommand, HASH_SCHEMES


#: Default hash scheme. Although iRODS provides alternatives, the whole of `snappy` pipeline uses MD5.
DEFAULT_HASH_SCHEME = "MD5"


class FindFilesCommon:
    """Class contains common methods used to find files."""

    def __init__(self, sheet):
        """Constructor.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.shortcuts.GermlineCaseSheet or biomedsheets.shortcuts.CancerCaseSheet
        """
        self.sheet = sheet

    def parse_sample_sheet(self):
        """Parse sample sheet.

        :return: Returns list of library names - used to define directory names though out the pipeline.
        """
        # Initialise variables
        library_names = []
        # Iterate over sample sheet
        if isinstance(self.sheet, shortcuts.GermlineCaseSheet):  # Germline
            for pedigree in self.sheet.cohort.pedigrees:
                for donor in pedigree.donors:
                    library_names.append(donor.dna_ngs_library.name)
        elif isinstance(self.sheet, shortcuts.CancerCaseSheet):  # Cancer
            for sample_pair in self.sheet.all_sample_pairs:
                if not (
                    sample_pair.tumor_sample.dna_ngs_library
                    and sample_pair.normal_sample.dna_ngs_library
                ):
                    logger.info(
                        f"Sample pair for cancer bio sample {sample_pair.tumor_sample.name} has is missing primary"
                        f"normal or primary cancer NGS library."
                    )
                    continue
                library_names.append(sample_pair.tumor_sample.dna_ngs_library.name)
                library_names.append(sample_pair.normal_sample.dna_ngs_library.name)
                # Check for RNA
                if sample_pair.tumor_sample.rna_ngs_library:
                    library_names.append(sample_pair.tumor_sample.rna_ngs_library.name)
                if sample_pair.normal_sample.rna_ngs_library:
                    library_names.append(sample_pair.normal_sample.rna_ngs_library.name)
        elif isinstance(self.sheet, shortcuts.GenericSampleSheet):  # Generic|RNA
            for ngs_library in self.sheet.all_ngs_libraries:
                extraction_type = ngs_library.test_sample.extra_infos["extractionType"]
                if extraction_type.lower() == "rna":
                    library_names.append(ngs_library.name)
        # Return list of library names
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
                f"Path to directory linked to raw data does not exist. Expected: {self.inlink_dir_path}"
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
                "Step list cannot be empty. Example of expected input: ['ngs_mapping', 'variant_calling']"
            )
        self.step_list = step_list

    def run(self):
        """Runs class routines.

        :return: Returns dictionary of dictionaries: key: step name (e.g., 'variant_calling'); value: dictionary
        with file structure per library/sample.
        """
        logger.info("Starting local files search ...")
        logger.info("...this may take several minutes...")

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
                    f"Canonical path for step '{step}' does not exist. Expected: {str(tmp_path)}"
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


class FindRemoteFiles(IrodsCheckCommand, FindFilesCommon):
    """Class finds and lists remote files associated with samples."""

    def __init__(self, args, sheet, sodar_url, sodar_api_token, assay_uuid, project_uuid):
        """Constructor.

        :param sodar_url: SODAR url.
        :type sodar_url: str

        :param sodar_api_token: SODAR API token.
        :type sodar_api_token: str

        :param assay_uuid: Assay UUID.
        :type assay_uuid: str

        :param project_uuid: Project UUID.
        :type project_uuid: str
        """
        IrodsCheckCommand.__init__(self, args=args)
        FindFilesCommon.__init__(self, sheet=sheet)
        self.sodar_url = sodar_url
        self.sodar_api_token = sodar_api_token
        self.assay_uuid = assay_uuid
        self.project_uuid = project_uuid

    def perform(self):
        """Perform class routines.

        :return: Returns dictionary of dictionaries: key: library name (e.g., 'P001-N1-DNA1-WES1'); value: dictionary
        with list of files (values) per remote directory (key).
        """
        logger.info("Starting remote files search ...")

        # Get assay iRODS path
        assay_path = self.get_assay_irods_path(assay_uuid=self.assay_uuid)

        # Get iRODS collection
        irods_collection_dict = self.retrieve_irods_data_objects(irods_path=assay_path)

        logger.info("... done with remote files search.")
        return irods_collection_dict

    def get_assay_irods_path(self, assay_uuid=None):
        """Get Assay iRODS path.

        :param assay_uuid: Assay UUID.
        :type assay_uuid: str [optional]

        :return: Returns Assay iRODS path - extracted via SODAR API.
        """
        investigation = api.samplesheet.retrieve(
            sodar_url=self.sodar_url,
            sodar_api_token=self.sodar_api_token,
            project_uuid=self.project_uuid,
        )
        for study in investigation.studies.values():
            if assay_uuid:
                logger.info(f"Using provided Assay UUID: {assay_uuid}")
                try:
                    assay = study.assays[assay_uuid]
                    return assay.irods_path
                except KeyError:
                    logger.error("Provided Assay UUID is not present in the Study.")
                    raise
            else:
                # Assumption: there is only one assay per study for `snappy` projects.
                # If multi-assay project it will only consider the first one and throw a warning.
                assays_ = list(study.assays.keys())
                if len(assays_) > 1:
                    self.multi_assay_warning(assays=assays_)
                for _assay_uuid in assays_:
                    assay = study.assays[_assay_uuid]
                    return assay.irods_path
        return None

    @staticmethod
    def multi_assay_warning(assays):
        """Display warning for multi-assay study.

        :param assays: Assays UUIDs as found in Studies.
        :type assays: list
        """
        multi_assay_str = "\n".join(assays)
        logger.warn(
            f"Project contains multiple Assays, will only consider UUID '{assays[0]}'.\n"
            f"All available UUIDs:\n{multi_assay_str}"
        )

    def retrieve_irods_data_objects(self, irods_path):
        """Retrieve data objects from iRODS.

        :param irods_path:
        :return:
        """
        # Connect to iRODS
        with self._get_irods_sessions() as irods_sessions:
            try:
                root_coll = irods_sessions[0].collections.get(irods_path)
                s_char = "s" if len(irods_sessions) != 1 else ""
                logger.info(f"{len(irods_sessions)} iRODS connection{s_char} initialized")
            except Exception as e:
                logger.error("Failed to retrieve iRODS path: %s", self.get_irods_error(e))
                raise

            # Get files and run checks
            logger.info("Querying for data objects")
            irods_collection = self.get_data_objs(root_coll)
            return self.parse_irods_collection(irods_collection=irods_collection)

    @staticmethod
    def parse_irods_collection(irods_collection):
        """

        :param irods_collection: iRODS collection.
        :type irods_collection: dict

        :return: Returns dictionary version of iRODS collection information. Key: File path in iRODS (str);
        Value: nested dict (Keys:  'irods_path', 'file_md5sum', 'replicas_md5sum').
        """
        # Initialise variables
        output_dict = {}
        checksums = irods_collection["checksums"]

        # Extract relevant info from iRODS collection: file and replicates MD5SUM
        for data_obj in irods_collection["files"]:
            chk_obj = checksums.get(data_obj.path + "." + DEFAULT_HASH_SCHEME.lower())
            with chk_obj.open("r") as f:
                file_sum = re.search(
                    HASH_SCHEMES[DEFAULT_HASH_SCHEME]["regex"], f.read().decode("utf-8")
                ).group(0)
                _tmp_dict = {
                    data_obj.name: {
                        "irods_path": data_obj.path,
                        "file_md5sum": file_sum,
                        "replicas_md5sum": [replica.checksum for replica in data_obj.replicas],
                    }
                }
                output_dict.update(_tmp_dict)
        return output_dict


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
            logger.error(f"Dictionary is empty for step '{check_name}'")
            return False

        # Restrict dictionary to directories associated with step
        subset_remote_files_dict = {}
        for key in self.remote_files_dict:
            path_ = self.remote_files_dict[key].get("irods_path")
            if check_name in path_:
                subset_remote_files_dict[key] = self.remote_files_dict.get(key)

        # Parse local files - remove library reference
        parsed_local_files_dict = dict(
            (key, val) for k in self.local_files_dict.values() for key, val in k.items()
        )

        # Compare dictionaries
        i_both, i_remote, i_local = self.compare_local_and_remote_files(
            local_dict=parsed_local_files_dict, remote_dict=subset_remote_files_dict
        )
        in_both_set.update(i_both)
        remote_only_set.update(i_remote)
        local_only_set.update(i_local)

        # MD5 check and report
        if self.check_md5:
            okay_list, different_list = self.compare_md5_files(
                remote_dict=subset_remote_files_dict, in_both_set=in_both_set
            )
            self.report_md5(okay_list, different_list)

        # Report
        self.report_findings(
            both_locations=in_both_set, only_local=local_only_set, only_remote=remote_only_set
        )

        # Return all okay
        return True

    @staticmethod
    def compare_md5_files(remote_dict, in_both_set):
        """Compares remote and local MD5 files.

        :param remote_dict: Dictionary with remote file structure. Key: remote directory path; Value: list of file
        names.
        :type remote_dict: dict

        :param in_both_set: Set with files found both locally and in remote directory.
        :type in_both_set: set

        :return: Returns tuple with: set of files that are present with same checksum locally and remote (local path);
        list of tuple of files with different checksum (local , remote path); and dictionary of files with the exact
        same checksum (excluding empty files) - key: checksum; values: list of remote paths.
        """
        # Initialise variables
        same_md5_list = []
        different_md5_list = []

        # Define expected MD5 files - report files where missing
        all_expected_local_md5 = [file_ + ".md5" for file_ in in_both_set]
        all_local_md5 = [file_ for file_ in all_expected_local_md5 if os.path.isfile(file_)]
        missing_list = set(all_expected_local_md5) - set(all_local_md5)

        if len(missing_list) > 0:
            missing_str = "\n".join(missing_list)
            logger.warn(
                f"Comparison was not possible for the case(s) below, MD5 file(s) expected but not "
                f"found locally:\n{missing_str}"
            )

        # Compare
        for md5_file in all_local_md5:
            file_name = os.path.basename(md5_file)
            original_file_name = file_name.replace(".md5", "")
            # Read local MD5
            with open(md5_file, "r", encoding="utf8") as f:
                local_md5 = f.readline()
                # Expected format example:
                # `459db8f7cb0d3a23a38fdc98286a9a9b  out.vcf.gz`
                local_md5 = local_md5.split(" ")[0]
            # Compare to remote MD5
            remote_md5_dict = remote_dict.get(original_file_name)
            remote_md5 = remote_md5_dict.get("file_md5sum")
            if local_md5 != remote_md5:
                different_md5_list.append(
                    (md5_file.replace(".md5", ""), remote_md5_dict.get("irods_path"))
                )
            else:
                same_md5_list.append(md5_file.replace(".md5", ""))
            # BONUS - check remote replicas
            if not all(
                (
                    replica_md5 == remote_md5
                    for replica_md5 in remote_md5_dict.get("replicas_md5sum")
                )
            ):
                logger.error(
                    f"iRODS metadata checksum not consistent with checksum file...\n"
                    f"File: {remote_md5_dict.get('irods_path')}\n"
                    f"File checksum: {remote_md5_dict.get('file_md5sum')}\n"
                    f"Metadata checksum: {', '.join(remote_md5_dict.get('replicas_md5sum'))}\n"
                )

        return same_md5_list, different_md5_list

    @staticmethod
    def compare_local_and_remote_files(local_dict, remote_dict):
        """Compare locally and remotely available files.

        :param local_dict: Dictionary with local file structure. Key: directory path; Value: list of file names. Paths
        in dictionary are expected to have an extra `output` subdirectory.
        :type local_dict: dict

        :param remote_dict: Dictionary with remote file structure. Key: remote directory path; Value: list of file
        names.
        :type remote_dict: dict

        :return: Returns tuple with three sets: one for files that are found both locally and remotely; one for files
        only found remotely; and, one for files only found locally.
        """
        # Initialise variables
        all_local_files_set = set()
        in_both_set = set()
        only_remote_set = set()
        only_local_set = set()
        file_to_local_path_dict = defaultdict(list)

        # Get all files remote
        all_remote_files_set = set(remote_dict.keys())

        # Get all files local
        for local_dir in local_dict:
            for file_ in local_dict.get(local_dir):
                if file_.endswith(".md5"):
                    continue
                file_to_local_path_dict[file_].append(local_dir)
                all_local_files_set.add(file_)

        # Present in both - stores only local path
        for file_ in all_remote_files_set.intersection(all_local_files_set):
            in_both_set.update(
                [local + "/" + file_ for local in file_to_local_path_dict.get(file_)]
            )

        # Only remote
        for file_ in all_remote_files_set - all_local_files_set:
            only_remote_set.add(remote_dict[file_].get("irods_path"))

        # Only local
        for file_ in all_local_files_set - all_remote_files_set:
            only_local_set.update(
                [local + "/" + file_ for local in file_to_local_path_dict.get(file_)]
            )

        return in_both_set, only_remote_set, only_local_set

    @staticmethod
    def report_md5(okay_list, different_list):
        """Report MD5 findings.

        :param okay_list: Set with all files with the exact same MD5 value - local path.
        :type okay_list: list

        :param different_list: List of tuples with files that are different locally
        and remotely - (local path, remote path).
        :type different_list: list
        """
        # Report same md5
        if len(okay_list) > 0:
            okay_str = "\n".join(sorted(okay_list))
            logger.info(f"Files with SAME MD5 locally and remotely:\n{okay_str}")

        # Report different md5
        if len(different_list) > 0:
            different_str = "\n".join(
                ["; i:".join(pair) for pair in sorted(different_list, key=lambda tup: tup[0])]
            )
            logger.warn(f"Files with DIFFERENT MD5 locally and remotely:\n{different_str}")

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
        in_both_str = "\n".join(sorted(both_locations))
        remote_only_str = "\n".join(sorted(only_remote))
        local_only_str = "\n".join(sorted(only_local))
        dashed_line = "-" * 25

        # Log
        if len(both_locations) > 0:
            logger.info(f"Files found BOTH locally and remotely:\n{in_both_str}")
        else:
            logger.warn("No file was found both locally and remotely.")
        if len(only_remote) > 0:
            logger.warn(f"Files found ONLY REMOTELY:\n{remote_only_str}")
        else:
            logger.info("No file found only remotely.")
        if len(only_local) > 0:
            logger.warn(f"Files found ONLY LOCALLY:\n{local_only_str}\n{dashed_line}")
        else:
            logger.info(f"No file found only locally.\n{dashed_line}")


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


class SomaticVariantCallingChecker(Checker):
    """Check for somatic variant calling results being present without checking content"""

    #: Step name being checked.
    check_name = "somatic_variant_calling"

    def __init__(self, *args, **kwargs):
        """ Constructor."""
        super().__init__(*args, **kwargs)

    def run(self):
        """Executes checks for Somatic Variant Calling files."""
        logger.info("Starting somatic_variant_calling checks ...")
        out_flag = self.coordinate_run(check_name=self.check_name)
        logger.info("... done with somatic_variant_calling checks.")
        return out_flag


class SnappyCheckRemoteCommand:
    """Implementation of the ``check-remote`` command."""

    def __init__(self, args):
        # Command line arguments.
        self.args = args
        # Find biomedsheet file
        self.biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=args.project_uuid
        )
        # Raw sample sheet.
        self.sheet = load_sheet_tsv(self.biomedsheet_tsv, args.tsv_shortcut)
        # Shortcut sample sheet.
        if args.tsv_shortcut == "cancer":
            self.shortcut_sheet = shortcuts.CancerCaseSheet(self.sheet)
        elif args.tsv_shortcut == "germline":
            self.shortcut_sheet = shortcuts.GermlineCaseSheet(self.sheet)
        else:  # generic
            self.shortcut_sheet = shortcuts.GenericSampleSheet(self.sheet)

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
            choices=("cancer", "generic", "germline"),
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
        parser.add_argument(
            "--md5",
            default=False,
            action="store_true",
            help="Flag to indicate if local and remote MD5 files should be compared.",
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

        logger.info("Starting cubi-tk snappy check-remote")
        logger.info("  args: %s", self.args)

        # Split execution between Cancer and Germline
        if self.args.tsv_shortcut == "cancer":
            variant_call_type = "somatic_variant_calling"
            variant_caller_class = SomaticVariantCallingChecker
        else:
            variant_call_type = "variant_calling"
            variant_caller_class = VariantCallingChecker

        # Find all remote files (iRODS)
        pseudo_args = SimpleNamespace(hash_scheme=DEFAULT_HASH_SCHEME)
        library_remote_files_dict = FindRemoteFiles(
            pseudo_args,
            self.shortcut_sheet,
            self.args.sodar_url,
            self.args.sodar_api_token,
            self.args.assay_uuid,
            self.args.project_uuid,
        ).perform()

        # Find all local files (canonical paths)
        library_local_files_dict = FindLocalFiles(
            sheet=self.shortcut_sheet,
            base_path=self.args.base_path,
            step_list=["ngs_mapping", variant_call_type],
        ).run()

        # Run checks
        results = [
            RawDataChecker(
                sheet=self.shortcut_sheet,
                base_path=self.args.base_path,
                remote_files_dict=library_remote_files_dict,
                local_files_dict={},  # special case: dict correctly defined inside class
                check_md5=self.args.md5,
            ).run(),
            NgsMappingChecker(
                remote_files_dict=library_remote_files_dict,
                local_files_dict=library_local_files_dict.get("ngs_mapping"),
                check_md5=self.args.md5,
            ).run(),
            variant_caller_class(
                remote_files_dict=library_remote_files_dict,
                local_files_dict=library_local_files_dict.get(variant_call_type),
                check_md5=self.args.md5,
            ).run(),
        ]
        if all(results):
            logger.info("All done.")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-remote``."""
    return SnappyCheckRemoteCommand.setup_argparse(parser)
