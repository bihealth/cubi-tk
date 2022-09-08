"""``cubi-tk snappy pull-raw-data``: pull raw data (i.e., FASTQ files) from SODAR iRODS to SNAPPY dataset directory.
More Information
----------------
- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-raw-data --help`` for more information.
- `SNAPPY Pipeline Documentation <https://snappy-pipeline.readthedocs.io/en/latest/>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""

import argparse
from collections import defaultdict
import os
import pathlib
from types import SimpleNamespace
import typing

import attr
from logzero import logger
import yaml

from .common import find_snappy_root_dir, get_biomedsheet_path, load_sheet_tsv
from ..common import load_toml_config
from .parse_sample_sheet import ParseSampleSheet
from .pull_data_common import PullDataCommon
from .retrieve_irods_collection import RetrieveIrodsCollection, DEFAULT_HASH_SCHEME


@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the pull-raw-data."""

    base_path: str
    verbose: bool
    sodar_server_url: str
    sodar_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    tsv_shortcut: str
    use_library_name: bool
    overwrite: bool
    dry_run: bool
    first_batch: int
    last_batch: int
    samples: typing.List[str]
    assay_uuid: str
    project_uuid: str


class PullRawDataCommand(PullDataCommon):
    """Implementation of the ``snappy pull-raw-data`` command."""

    #: File type dictionary. Key: file type; Value: additional expected extensions (tuple).
    file_type_to_extensions_dict = {"fastq": ("fastq.gz",)}

    def __init__(self, config: Config):
        PullDataCommon.__init__(self)
        #: Command line arguments.
        self.config = config

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )
        parser.add_argument(
            "--base-path",
            default=os.getcwd(),
            required=False,
            help=(
                "Base path of project (contains '.snappy_pipeline/' etc.), spiders up from current "
                "work directory and falls back to current working directory by default."
            ),
        )
        group_sodar = parser.add_argument_group("SODAR-related")
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )
        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., just displays the files that would be downloaded.",
        )
        parser.add_argument(
            "--overwrite", default=False, action="store_true", help="Allow overwriting of files"
        )
        parser.add_argument(
            "--tsv-shortcut",
            default="germline",
            choices=("cancer", "generic", "germline"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--first-batch", default=0, type=int, help="First batch to be transferred. Defaults: 0."
        )
        parser.add_argument(
            "--last-batch", type=int, required=False, help="Last batch to be transferred."
        )
        parser.add_argument("--samples", help="Optional list of samples to pull")
        parser.add_argument(
            "--use-library-name",
            default=False,
            action="store_true",
            help=(
                "Flag to indicate that the search in SODAR directories should be based on library name "
                "(e.g. 'P001-N1-DNA1-WGS1') instead of sample identifier (e.g.'P001') in the file name."
            ),
        )
        parser.add_argument(
            "--assay-uuid",
            dest="assay_uuid",
            default=None,
            help="UUID of assay to create landing zone for.",
        )
        parser.add_argument("project_uuid", help="UUID of project to download data for.")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        # If SODAR info not provided, fetch from user's toml file
        toml_config = load_toml_config(args)
        args.sodar_url = args.sodar_url or toml_config.get("global", {}).get("sodar_server_url")
        args.sodar_api_token = args.sodar_api_token or toml_config.get("global", {}).get(
            "sodar_api_token"
        )
        args = vars(args)
        # Adjust `base_path` to snappy root
        args["base_path"] = find_snappy_root_dir(args["base_path"])
        # Remove unnecessary arguments
        args.pop("config", None)
        args.pop("cmd", None)
        args.pop("snappy_cmd", None)
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the download."""
        logger.info("Loading configuration file and look for dataset")

        # Find download path
        download_path = self._get_download_path()
        if not download_path:
            return 1
        logger.info(f"=> will download to {download_path}")

        # Get sample sheet
        biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.config.base_path, uuid=self.config.project_uuid
        )
        sheet = load_sheet_tsv(biomedsheet_tsv, self.config.tsv_shortcut)

        # Filter requested samples and folder directories
        parser = ParseSampleSheet()
        if self.config.use_library_name:
            selected_identifiers_tuples = list(
                parser.yield_ngs_library_and_folder_names(
                    sheet=sheet,
                    min_batch=self.config.first_batch,
                    max_batch=self.config.last_batch,
                    selected_ids=self.config.samples,
                )
            )
        else:
            selected_identifiers_tuples = list(
                parser.yield_sample_and_folder_names(
                    sheet=sheet,
                    min_batch=self.config.first_batch,
                    max_batch=self.config.last_batch,
                    selected_ids=self.config.samples,
                )
            )
        selected_identifiers = [pair[0] for pair in selected_identifiers_tuples]

        # Get assay UUID if not provided
        assay_uuid = None
        if not self.config.assay_uuid:
            assay_uuid = self.get_assay_uuid(
                sodar_url=self.config.sodar_url,
                sodar_api_token=self.config.sodar_api_token,
                project_uuid=self.config.project_uuid,
            )

        # Find all remote files (iRODS)
        pseudo_args = SimpleNamespace(hash_scheme=DEFAULT_HASH_SCHEME)
        remote_files_dict = RetrieveIrodsCollection(
            pseudo_args,
            self.config.sodar_url,
            self.config.sodar_api_token,
            self.config.assay_uuid,
            self.config.project_uuid,
        ).perform()

        # Filter based on identifiers and file type
        if self.config.use_library_name:
            filtered_remote_files_dict = self.filter_irods_collection_by_library_name_in_path(
                identifiers=selected_identifiers,
                remote_files_dict=remote_files_dict,
                file_type="fastq",
            )
        else:
            filtered_remote_files_dict = self.filter_irods_collection(
                identifiers=selected_identifiers,
                remote_files_dict=remote_files_dict,
                file_type="fastq",
            )
        if len(filtered_remote_files_dict) == 0:
            extensions = self.file_type_to_extensions_dict.get("fastq")
            remote_files_fastq = [
                file_ for file_ in remote_files_dict if file_.endswith(extensions)
            ]
            self.report_no_file_found(available_files=remote_files_fastq)
            return 0

        # Pair iRODS path with output path
        library_to_irods_dict = self.get_library_to_irods_dict(
            identifiers=selected_identifiers, remote_files_dict=filtered_remote_files_dict
        )
        path_pair_list = self.pair_ipath_with_outdir(
            library_to_irods_dict=library_to_irods_dict,
            identifiers_tuples=selected_identifiers_tuples,
            output_dir=download_path,
            assay_uuid=self.config.assay_uuid or assay_uuid,
        )

        # Retrieve files from iRODS or print
        if not self.config.dry_run:
            self.get_irods_files(
                irods_local_path_pairs=path_pair_list, force_overwrite=self.config.overwrite
            )
        else:
            self._report_files(
                irods_local_path_pairs=path_pair_list, identifiers=selected_identifiers
            )

        logger.info("All done. Have a nice day!")
        return 0

    def filter_irods_collection_by_library_name_in_path(
        self, identifiers, remote_files_dict, file_type
    ):
        """Filter iRODS collection based on identifiers and file type/extension.

        Assumes that SODAR directories follow the logic below to filter by library name:
        /sodarZone/projects/../<PROJECT_UUID>/sample_data/study_<STUDY_UUID>/assay_<ASSAY_UUID>/<LIBRARY_NAME>

        :param identifiers: List of sample identifiers or library names.
        :type identifiers: list

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1.vcf.gz'); Value: iRODS data (``IrodsDataObject``).
        :type remote_files_dict: dict

        :param file_type: File type, example: 'fastq'.
        :type file_type: str

        :return: Returns filtered iRODS collection dictionary.
        """
        # Initialise variables
        output_dict = defaultdict(list)
        extensions_tuple = self.file_type_to_extensions_dict.get(file_type)

        # Iterate
        for key, value in remote_files_dict.items():
            # Initialise variables
            _irods_path_list = []
            # Simplify criteria: must have the correct file extension
            if not key.endswith(extensions_tuple):
                continue

            # Check for common links
            # Note: if a file with the same name is present in both assay and in a common file, it will be ignored.
            in_common_links = False
            for irods_obj in value:
                # Piggyback loop for dir check
                _irods_path_list.append(irods_obj.irods_path)
                # Actual check
                if self._irods_path_in_common_links(irods_obj.irods_path):
                    in_common_links = True
                    break

            # Update output if: not in common links and any id is part of SODAR path
            # Assumption: the path will include at most one library name
            if not in_common_links:
                all_directories = sum([path_.split("/") for path_ in _irods_path_list], [])
                for id_ in identifiers:
                    if any([id_ == dir_ for dir_ in all_directories]):
                        output_dict[id_].extend(value)
                        break

        return output_dict

    @staticmethod
    def pair_ipath_with_outdir(library_to_irods_dict, identifiers_tuples, assay_uuid, output_dir):
        """Pair iRODS path with local output directory

        :param library_to_irods_dict: Dictionary with iRODS collection information by sample. Key: sample name as
        string (e.g., 'P001'); Value: iRODS data (``IrodsDataObject``).
        :type library_to_irods_dict: dict

        :param identifiers_tuples: List of tuples (sample name, folder name) as defined in the sample sheet.
        :type identifiers_tuples: List[Tuple[str, str]]

        :param output_dir: Output directory path.
        :type output_dir: str

        :param assay_uuid: Assay UUID - used as a hack to get the directory structure in SODAR.
        :type assay_uuid: str

        :return: Return list of tuples (iRODS path [str], local output directory [str]).
        """
        # Initiate output
        output_list = []
        # Iterate over samples and iRODS objects
        for pair in identifiers_tuples:
            id_ = pair[0]
            folder_name = pair[1]
            irods_obj_list = library_to_irods_dict.get(id_)
            if not irods_obj_list:
                logger.warning(f"No files found for sample '{id_}'.")
                continue
            for irods_obj in irods_obj_list:
                # Keeps iRODS directory structure if assay UUID is provided.
                # Assumption is that SODAR directories follow the logic below:
                # /sodarZone/projects/../<PROJECT_UUID>/sample_data/study_<STUDY_UUID>/assay_<ASSAY_UUID>/<LIBRARY_NAME>
                try:
                    irods_dir_structure = os.path.dirname(
                        str(irods_obj.irods_path).split(f"assay_{assay_uuid}/")[1]
                    )
                    _out_path = os.path.join(
                        output_dir, folder_name, irods_dir_structure, irods_obj.file_name
                    )
                except IndexError:
                    logger.warning(
                        f"Provided Assay UUID '{assay_uuid}' is not present in SODAR path, "
                        f"hence directory structure won't be preserved.\n"
                        f"All files will be stored in root of output directory: {output_list}"
                    )
                    _out_path = os.path.join(output_dir, folder_name, irods_obj.file_name)
                # Update output
                output_list.append((irods_obj.irods_path, _out_path))
                output_list.append((irods_obj.irods_path + ".md5", _out_path + ".md5"))

        return output_list

    @staticmethod
    def get_library_to_irods_dict(identifiers, remote_files_dict):
        """Get dictionary library name to iRODS object

        :param identifiers: List of selected identifiers, sample name.
        :type identifiers: list

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001_R1_001.fastq.gz'); Value: iRODS data (``IrodsDataObject``).
        :type remote_files_dict: dict

        :return: Returns dictionary: Key: identifier (sample name [str]); Value: list of iRODS objects.
        """
        out_dict = {}
        for id_ in identifiers:
            out_dict[id_] = sum(
                [remote_files_dict.get(key) for key in remote_files_dict if id_ in key], []
            )
        return out_dict

    @staticmethod
    def _report_files(irods_local_path_pairs, identifiers):
        """Report iRODS files associated with identifiers (dry-run).

        :param irods_local_path_pairs: List of tuples (iRODS path [str], local output directory [str]).
        :type irods_local_path_pairs: List[Tuple[str, str]]

        :param identifiers: List of selected identifiers (sample names).
        :type identifiers: list
        """
        # Initiate variable
        library_to_irods_file = defaultdict(list)
        report_str = "Download files from SODAR (dry-run)\n\n"
        # Iterate over pairs
        for pair in irods_local_path_pairs:
            file_name = pair[0].split("/")[-1]
            for library_name in identifiers:
                if library_name in file_name:
                    library_to_irods_file[library_name].append(file_name)
                    break
        # Build string
        for library_name in identifiers:
            _template_str = f"- Library '{library_name}' has {{n_files}} files{{punctuation}}\n"
            file_list = library_to_irods_file.get(library_name, None)
            if not file_list:
                report_str += _template_str.format(n_files=0, punctuation=".")
            else:
                report_str += _template_str.format(n_files=len(file_list), punctuation=":")
                for file_ in sorted(file_list):
                    report_str += f"\t{file_}\n"
        # Report
        logger.info(report_str)

    def _get_download_path(self):
        """Get download path

        :return: Return path to download as defined in snappy configuration, i.e., raw data path.
        """
        # Find config file
        with (pathlib.Path(self.config.base_path) / ".snappy_pipeline" / "config.yaml").open(
            "rt"
        ) as inputf:
            config = yaml.safe_load(inputf)
        # Parse available datasets in config
        if "data_sets" not in config:
            logger.error(
                f"Could not find configuration key '{repr('data_sets')}' in {inputf.name}."
            )
            return
        for key, data_set in config["data_sets"].items():
            if (
                key == self.config.project_uuid
                or data_set.get("sodar_uuid") == self.config.project_uuid
            ):
                break
        else:  # no "break" out of for-loop
            logger.error(
                f"Could not find dataset with key/sodar_uuid entry of {self.config.project_uuid}"
            )
            return
        if not data_set.get("search_paths"):
            logger.error(f"Data set has no attribute {repr('search_paths')}")
            return

        return data_set["search_paths"][-1]


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy pull-raw-data``."""
    return PullRawDataCommand.setup_argparse(parser)
