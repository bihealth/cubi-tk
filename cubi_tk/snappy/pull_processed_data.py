import argparse
import os
from types import SimpleNamespace
import typing

from logzero import logger

from .common import get_biomedsheet_path, load_sheet_tsv
from ..common import load_toml_config
from .parse_sample_sheet import ParseSampleSheet
from .pull_data_common import PullDataCommon

from .retrieve_irods_collection import RetrieveIrodsCollection, DEFAULT_HASH_SCHEME


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
            "--first-batch", default=0, type=int, help="First batch to be transferred. Defaults: 0."
        )
        parser.add_argument(
            "--last-batch", type=int, required=False, help="Last batch to be transferred."
        )
        parser.add_argument(
            "--output-directory",
            default=None,
            required=True,
            help="Output directory, where downloaded files will be stored.",
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
            logger.error(f"Base path does not exist: {args.base_path}")
            res = 1

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
        logger.info("  args: %s", self.args)

        # Find biomedsheet file
        biomedsheet_tsv = get_biomedsheet_path(
            start_path=self.args.base_path, uuid=self.args.project_uuid
        )
        # Raw sample sheet.
        sheet = load_sheet_tsv(biomedsheet_tsv, self.args.tsv_shortcut)

        # Filter requested samples or libraries
        parser = ParseSampleSheet()
        if self.args.sample_id:  # example: 'P001'
            selected_identifiers = list(
                parser.yield_sample_names(
                    sheet=sheet, min_batch=self.args.first_batch, max_batch=self.args.last_batch
                )
            )
        else:  # example: 'P001-N1-DNA1-WGS1'
            selected_identifiers = list(
                parser.yield_ngs_library_names(
                    sheet=sheet, min_batch=self.args.first_batch, max_batch=self.args.last_batch
                )
            )

        # Get assay UUID if not provided
        assay_uuid = None
        if not self.args.assay_uuid:
            assay_uuid = self.get_assay_uuid(
                sodar_url=self.args.sodar_url,
                sodar_api_token=self.args.sodar_api_token,
                project_uuid=self.args.project_uuid,
            )

        # Find all remote files (iRODS)
        pseudo_args = SimpleNamespace(hash_scheme=DEFAULT_HASH_SCHEME)
        remote_files_dict = RetrieveIrodsCollection(
            pseudo_args,
            self.args.sodar_url,
            self.args.sodar_api_token,
            self.args.assay_uuid,
            self.args.project_uuid,
        ).perform()

        # Filter based on identifiers and file type
        filtered_remote_files_dict = self.filter_irods_collection(
            identifiers=selected_identifiers,
            remote_files_dict=remote_files_dict,
            file_type=self.args.file_type,
        )
        if len(filtered_remote_files_dict) == 0:
            if len(remote_files_dict) > 50:
                limited_str = " (limited to first 50)"
                ellipsis_ = "..."
                remote_files_str = "\n".join([*remote_files_dict][:50])
            else:
                limited_str = ""
                ellipsis_ = ""
                remote_files_str = "\n".join([*remote_files_dict])

            logger.warning(
                f"No file was found using the selected criteria.\n"
                f"Available files{limited_str}:\n{remote_files_str}\n{ellipsis_}"
            )
            return 0

        # Pair iRODS path with output path
        path_pair_list = self.pair_ipath_with_outdir(
            remote_files_dict=filtered_remote_files_dict,
            output_dir=self.args.output_directory,
            assay_uuid=self.args.assay_uuid or assay_uuid,
        )

        # Retrieve files from iRODS
        self.get_irods_files(irods_local_path_pairs=path_pair_list)

        logger.info("All done. Have a nice day!")
        return 0

    @staticmethod
    def pair_ipath_with_outdir(remote_files_dict, output_dir, assay_uuid):
        """Pair iRODS path with local output directory

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1'); Value: iRODS data (``IrodsDataObject``).
        :type remote_files_dict: dict

        :param output_dir: Output directory path.
        :type output_dir: str

        :param assay_uuid: Assay UUID - used as a hack to get the directory structure in SODAR.
        :type assay_uuid: str

        :return: Return list of tuples (iRODS path [str], local output directory [str]).
        """
        # Initiate output
        output_list = []
        # Iterate over iRODS objects
        for irods_obj_list in remote_files_dict.values():
            for irods_obj in irods_obj_list:
                # Keeps iRODS directory structure if assay UUID is provided.
                # Assumption is that SODAR directories follow the logic below:
                # /sodarZone/projects/../<PROJECT_UUID>/sample_data/study_<STUDY_UUID>/assay_<ASSAY_UUID>/<LIBRARY_NAME>
                try:
                    irods_dir_structure = os.path.dirname(
                        str(irods_obj.irods_path).split(f"assay_{assay_uuid}/")[1]
                    )
                    _out_path = os.path.join(output_dir, irods_dir_structure, irods_obj.file_name)
                except IndexError:
                    logger.warning(
                        f"Provided Assay UUID '{assay_uuid}' is not present in SODAR path, "
                        f"hence directory structure won't be preserved.\n"
                        f"All files will be stored in root of output directory: {output_list}"
                    )
                    _out_path = os.path.join(output_dir, irods_obj.file_name)
                # Update output
                output_list.append((irods_obj.irods_path, _out_path))
                output_list.append((irods_obj.irods_path + ".md5", _out_path + ".md5"))

        return output_list


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk sodar pull-processed-data``."""
    return PullProcessedDataCommand.setup_argparse(parser)
