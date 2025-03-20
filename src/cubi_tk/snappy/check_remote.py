"""``cubi-tk snappy check-remote``: check that files are present in remote SODAR/iRODS.

Only uses local information for checking that the linked-in RAW data files are correct in terms
of the MD5 sum.  Otherwise, just checks for presence of files (for now), the rationale being that

"""
import argparse
from collections import defaultdict
import os
from pathlib import Path
import typing

from biomedsheets import shortcuts
from loguru import logger

from cubi_tk.parsers import print_args

from ..sodar_common import RetrieveSodarCollection
from .common import get_biomedsheet_path, load_sheet_tsv


class FindFilesCommon:
    """Class contains common methods used to find files."""

    def __init__(self, sheet):
        """Constructor.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.shortcuts.GermlineCaseSheet or biomedsheets.shortcuts.CancerCaseSheet
        """
        self.sheet = sheet

    def parse_sample_sheet(self):  #noqa: C901
        """Parse sample sheet.

        :return: Returns list of library names - used to define directory names though out the pipeline.
        """
        # Initialise variables
        library_names = []
        # Iterate over sample sheet
        if isinstance(self.sheet, shortcuts.GermlineCaseSheet):  # Germline
            for pedigree in self.sheet.cohort.pedigrees:
                for donor in pedigree.donors:
                    if donor.dna_ngs_library:
                        library_names.append(donor.dna_ngs_library.name)
                    else:
                        logger.warning(f"Skipping - no NGS library associated with {donor.name}")
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
                logger.warning(
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


class Checker:
    """Class with common checker methods."""

    def __init__(self, local_files_dict, remote_files_dict, check_md5=False):
        """Constructor.

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
            if all((check_name in dat.path for dat in self.remote_files_dict[key])):
                subset_remote_files_dict[key] = self.remote_files_dict.get(key)

        # Parse local files - remove library reference
        parsed_local_files_dict = {(key, val) for k in self.local_files_dict.values() for key, val in k.items()}

        # Compare dictionaries
        i_both, i_remote, i_local = self.compare_local_and_remote_files(
            local_dict=parsed_local_files_dict, remote_dict=subset_remote_files_dict
        )
        in_both_set.update(i_both)
        remote_only_set.update(i_remote)
        local_only_set.update(i_local)

        # Report
        self.report_multiple_file_versions_in_sodar(remote_dict=subset_remote_files_dict)
        if self.check_md5:  # md5 check report
            okay_list, different_list = self.compare_md5_files(
                remote_dict=subset_remote_files_dict, in_both_set=in_both_set
            )
            self.report_findings_md5(okay_list, different_list)
        else:  # simple report
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
            logger.warning(
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
            for irods_dat in remote_dict.get(original_file_name):
                if local_md5 != irods_dat.FILE_MD5SUM:
                    different_md5_list.append((md5_file.replace(".md5", ""), irods_dat.path))
                else:
                    same_md5_list.append(md5_file.replace(".md5", ""))
                # BONUS - check remote replicas
                if not all(
                    (
                        replica_md5 == irods_dat.FILE_MD5SUM
                        for replica_md5 in irods_dat.REPLICAS_MD5SUM
                    )
                ):
                    logger.error(
                        f"iRODS metadata checksum not consistent with checksum file...\n"
                        f"File: {irods_dat.path}\n"
                        f"File checksum: {irods_dat.FILE_MD5SUM}\n"
                        f"Metadata checksum: {', '.join(irods_dat.REPLICAS_MD5SUM)}\n"
                    )

        return same_md5_list, different_md5_list

    @staticmethod
    def compare_local_and_remote_files(local_dict, remote_dict):
        """Compare locally and remotely available files.

        :param local_dict: Dictionary with local file structure. Key: directory path; Value: list of file names. Paths
        in dictionary are expected to have an extra `output` subdirectory.
        :type local_dict: dict

        :param remote_dict: Dictionary with remote file structure. Key: file name; Value: list of IRodsDataObject.
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
            for irods_dat in remote_dict[file_]:
                only_remote_set.add(irods_dat.path)

        # Only local
        for file_ in all_local_files_set - all_remote_files_set:
            only_local_set.update(
                [local + "/" + file_ for local in file_to_local_path_dict.get(file_)]
            )

        return in_both_set, only_remote_set, only_local_set

    @staticmethod
    def report_multiple_file_versions_in_sodar(remote_dict):
        """Report if a file has multiple verions in SODAR.

        Relevant if raw file if for example there is an error in one of the sequences, same name but different dates:
        - raw_data/2022-01-25/<SAMPLE_ID>_R2_001.fastq.gz
        - raw_data/2022-05-06/<SAMPLE_ID>_R2_001.fastq.gz


        :param remote_dict: Dictionary with remote file structure. Key: file name; Value: list of IRodsDataObject.
        :type remote_dict: dict
        """
        # Build inner dictionary with relevant information to be displayed
        inner_dict = {}
        for file_, irods_list in remote_dict.items():
            if len(irods_list) > 1:
                inner_dict[file_] = [dat.path for dat in irods_list]
        # Format and display information - if any
        if len(inner_dict) > 0:
            pairs_str = ""
            for key, value in inner_dict.items():
                irods_paths_str = "\n".join(value)
                _tmp_str = f"\n>> {key}\n{irods_paths_str}"
                pairs_str += _tmp_str
            logger.warning(f"Files with different versions in SODAR:{pairs_str}")

    @staticmethod
    def report_findings_md5(okay_list, different_list):
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
            logger.warning(f"Files with DIFFERENT MD5 locally and remotely:\n{different_str}")

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
            logger.warning("No file was found both locally and remotely.")
        if len(only_remote) > 0:
            logger.warning(f"Files found ONLY REMOTELY:\n{remote_only_str}")
        else:
            logger.info("No file found only remotely.")
        if len(only_local) > 0:
            logger.warning(f"Files found ONLY LOCALLY:\n{local_only_str}\n{dashed_line}")
        else:
            logger.info(f"No file found only locally.\n{dashed_line}")


class RawDataChecker(Checker):
    """Check for raw data being present and equal as in local ``ngs_mapping`` directory."""

    #: Step name being checked.
    check_name = "raw_data"

    def __init__(self, sheet, base_path, local_files_dict, remote_files_dict, check_md5):
        """Constructor.

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
        """Constructor."""
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
        """Constructor."""
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
        """Constructor."""
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
            "--tsv-shortcut",
            default="germline",
            choices=("cancer", "generic", "germline"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--md5",
            default=False,
            action="store_true",
            help="Flag to indicate if local and remote MD5 files should be compared.",
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

        logger.info("Starting cubi-tk snappy check-remote")
        print_args(self.args)

        # Split execution between Cancer and Germline
        if self.args.tsv_shortcut == "cancer":
            variant_call_type = "somatic_variant_calling"
            variant_caller_class = SomaticVariantCallingChecker
        else:
            variant_call_type = "variant_calling"
            variant_caller_class = VariantCallingChecker

        # Find all remote files (iRODS)
        library_remote_files_dict = RetrieveSodarCollection(
            self.args
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
