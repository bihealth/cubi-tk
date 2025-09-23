"""``cubi-tk snappy pull-processed-data``: pull processed data from SODAR iRODS to output directory.
More Information
----------------
- Also see ``cubi-tk snappy`` :ref:`cli_main <CLI documentation>` and ``cubi-tk snappy pull-processed-data --help`` for more information.
- `SNAPPY Pipeline Documentation <https://snappy-pipeline.readthedocs.io/en/latest/>`__.
- `BiomedSheet Documentation <https://biomedsheets.readthedocs.io/en/master/>`__.
"""
import argparse
import os
import typing

from loguru import logger

from cubi_tk.parsers import print_args

from ..sodar_common import RetrieveSodarCollection
from .common import get_biomedsheet_path, load_sheet_tsv
from .parse_sample_sheet import ParseSampleSheet
from .pull_data_common import PullDataCommon

#: Valid file extensions
VALID_FILE_TYPES = ("bam", "vcf", "txt", "csv", "log")


class PullProcessedDataCommand(PullDataCommon):
    """Implementation of the ``pull-processed-data`` command."""

    #: File type dictionary. Key: file type; Value: additional expected extensions (tuple).
    file_type_to_extensions_dict = {
        "bam": ("bam", "bam.bai"),
        "vcf": ("vcf", "vcf.gz", "vcf.tbi", "vcf.gz.tbi"),
        "log": ("log", "conda_info.txt", "conda_list.txt"),
        "txt": ("txt",),
        "csv": ("csv",),
    }

    def __init__(self, args):
        PullDataCommon.__init__(self)
        # Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-remote`` command."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--sample-id",
            default=False,
            action="store_true",
            help=(
                "Flag to indicate if search should be based on sample identifier (e.g.'P001') "
                "instead of library name (e.g. 'P001-N1-DNA1-WGS1')."
            ),
        )
        parser.add_argument(
            "--file-type",
            default=None,
            required=True,
            choices=VALID_FILE_TYPES,
            type=str,
            help=f"File extensions to be retrieved. Valid options: {VALID_FILE_TYPES}",
        )
        parser.add_argument(
            "--download-all-versions",
            default=False,
            action="store_true",
            help=(
                "By default only the latest version of a file will be download. For instance, if a was uploaded "
                "two times, in '2022-01-31' and '2022-02-28', only the latest is downloaded. If this flag is "
                "present, both versions will be downloaded."
            ),
        )
    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments."""
        res = 0
        # Validate base path
        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error(f"Base path does not exist: {args.base_path}")
            res = 1

        if self.args.output_directory is None:
            logger.info('No --output-directory given, defaulting to CWD!')
            self.args.output_directory = os.getcwd()

        # Validate output directory path
        if not (
            os.path.exists(args.output_directory) and os.access(args.output_directory, os.W_OK)
        ):
            logger.error(
                f"Output directory path either does not exist or it is not writable: {args.base_path}"
            )
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy pull-processed-data")
        print_args(self.args)

        # Find biomedsheet file
        biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=self.args.project_uuid
        )
        # Raw sample sheet.
        sheet = load_sheet_tsv(biomedsheet_tsv, self.args.tsv_shortcut)

        # Filter requested samples or libraries
        if self.args.samples:
            selected_identifiers = self._filter_requested_samples_or_libraries_by_selected_samples(
                sheet=sheet,
                selected_samples=self.args.samples,
                by_sample_id=self.args.sample_id,
            )
        else:
            selected_identifiers = self._filter_requested_samples_or_libraries(
                sheet=sheet,
                min_batch=self.args.first_batch,
                max_batch=self.args.last_batch,
                by_sample_id=self.args.sample_id,
            )

        # Find all remote files (iRODS) and get assay UUID if not provided
        sodar_coll = RetrieveSodarCollection(
            self.args
        )
        remote_files_dict = sodar_coll.perform()
        self.args.assay_uuid = sodar_coll.get_assay_uuid()

        # Filter based on identifiers and file type
        filtered_remote_files_dict = self.filter_irods_collection(
            identifiers=selected_identifiers,
            remote_files_dict=remote_files_dict,
            file_type=self.args.file_type,
        )
        if len(filtered_remote_files_dict) == 0:
            self.report_no_file_found(available_files=[*remote_files_dict])
            return 0

        # Pair iRODS path with output path
        path_pair_list = self.pair_ipath_with_outdir(
            remote_files_dict=filtered_remote_files_dict,
            output_dir=self.args.output_directory,
            assay_uuid=self.args.assay_uuid,
            retrieve_all=self.args.download_all_versions,
        )

        # Retrieve files from iRODS
        self.get_irods_files(
            irods_local_path_pairs=path_pair_list, force_overwrite=self.args.overwrite, sodar_profile=self.args.config_profile
        )

        logger.info("All done. Have a nice day!")
        return 0

    @staticmethod
    def _filter_requested_samples_or_libraries_by_selected_samples(
        sheet, selected_samples, by_sample_id
    ):
        """Filter requested samples or libraries based on selected sample list

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param selected_samples: List of sample identifiers as string, e.g., 'P001,P002,P003'.
        :type selected_samples: str

        :param by_sample_id: Flag filter by sample id instead of library name.
        :type by_sample_id: bool

        :return: Returns filtered list of identifiers based on inputted parameters.
        """
        selected_samples_list = selected_samples.split(",")
        if by_sample_id:
            return selected_samples_list
        else:
            parser = ParseSampleSheet()
            return list(
                parser.yield_ngs_library_names_filtered_by_samples(
                    sheet=sheet, selected_samples=selected_samples_list
                )
            )

    @staticmethod
    def _filter_requested_samples_or_libraries(sheet, min_batch, max_batch, by_sample_id):
        """Filter requested samples or libraries

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: First batch number.
        :type min_batch:  int

        :param max_batch: Last batch number.
        :type max_batch: int

        :param by_sample_id: Flag filter by sample id instead of library name.
        :type by_sample_id: bool

        :return: Returns filtered list of identifiers based on inputted parameters.
        """
        parser = ParseSampleSheet()
        if by_sample_id:  # example: 'P001'
            yield_names_method = parser.yield_sample_names
        else:  # example: 'P001-N1-DNA1-WGS1'
            yield_names_method = parser.yield_ngs_library_names
        return list(yield_names_method(sheet=sheet, min_batch=min_batch, max_batch=max_batch))

    def pair_ipath_with_outdir(self, remote_files_dict, output_dir, assay_uuid, retrieve_all=False):
        """Pair iRODS path with local output directory

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1'); Value: iRODS data (``iRODSDataObject``).
        :type remote_files_dict: dict

        :param output_dir: Output directory path.
        :type output_dir: str

        :param assay_uuid: Assay UUID - used as a hack to get the directory structure in SODAR.
        :type assay_uuid: str

        :param retrieve_all: Flag indicates if all versions of the files should be downloaded (True)
        or just the latest (False). Default: False.
        :type retrieve_all: bool

        :return: Return list of tuples (iRODS path [str], local output directory [str]).
        """
        # Initiate output
        output_list = []
        # Iterate over iRODS objects
        for irods_obj_list in remote_files_dict.values():
            # Retrieve only latest, test if list is empty
            if irods_obj_list and not retrieve_all:
                irods_obj_list = [self.sort_irods_object_by_date_in_path(irods_obj_list)[0]]

            # Iterate over iRODS object list, by default list contain only latest
            for irods_obj in irods_obj_list:
                # Keeps iRODS directory structure if assay UUID is provided.
                # Assumption is that SODAR directories follow the logic below:
                # /sodarZone/projects/../<PROJECT_UUID>/sample_data/study_<STUDY_UUID>/assay_<ASSAY_UUID>/<LIBRARY_NAME>
                try:
                    irods_dir_structure = os.path.dirname(
                        str(irods_obj.path).split(f"assay_{assay_uuid}/")[1]
                    )
                    _out_path = os.path.join(output_dir, irods_dir_structure, irods_obj.name)
                except IndexError:
                    logger.warning(
                        f"Provided Assay UUID '{assay_uuid}' is not present in SODAR path, "
                        f"hence directory structure won't be preserved.\n"
                        f"All files will be stored in root of output directory: {output_list}"
                    )
                    _out_path = os.path.join(output_dir, irods_obj.name)
                # Update output
                output_list.append((irods_obj.path, _out_path))

        return output_list


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar pull-processed-data``."""
    return PullProcessedDataCommand.setup_argparse(parser)
