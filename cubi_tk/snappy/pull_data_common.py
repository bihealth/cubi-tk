from types import SimpleNamespace

from irods.keywords import FORCE_FLAG_KW
from irods.exception import OVERWRITE_WITHOUT_FORCE_FLAG

from logzero import logger
from pathlib import Path
from sodar_cli import api

from ..irods.check import IrodsCheckCommand
from .retrieve_irods_collection import DEFAULT_HASH_SCHEME


#: Valid file extensions
VALID_FILE_TYPES = ("bam", "vcf", "txt", "csv", "log")


class PullDataCommon(IrodsCheckCommand):
    """Implementation of common pull data methods."""

    #: File type dictionary. Key: file type; Value: additional expected extensions (tuple).
    file_type_to_extensions_dict = None

    def __init__(self):
        IrodsCheckCommand.__init__(self, args=SimpleNamespace(hash_scheme=DEFAULT_HASH_SCHEME))

    def filter_irods_collection(self, identifiers, remote_files_dict, file_type):
        """Filter iRODS collection based on identifiers (sample id or library name) and file type/extension.

        :param identifiers: List of sample identifiers or library names.
        :type identifiers: list

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1.vcf.gz'); Value: iRODS data (``IrodsDataObject``).
        :type remote_files_dict: dict

        :param file_type: File type, example: 'bam' or 'vcf'.
        :type file_type: str

        :return: Returns filtered iRODS collection dictionary.
        """
        # Initialise variables
        filtered_dict = {}
        extensions_tuple = self.file_type_to_extensions_dict.get(file_type)

        # Iterate
        for key, value in remote_files_dict.items():
            # Check for common links
            # Note: if a file with the same name is present in both assay and in a common file, it will be ignored.
            in_common_links = False
            for irods_obj in value:
                in_common_links = self._irods_path_in_common_links(irods_obj.irods_path)
                if in_common_links:
                    break

            # Filter
            if (
                any(id_ in key for id_ in identifiers)  # presence of identifiers
                and key.endswith(extensions_tuple)  # correct file extension
                and not in_common_links  # not in common links
            ):
                filtered_dict[key] = value

        return filtered_dict

    @staticmethod
    def _irods_path_in_common_links(irods_path):
        """Checks if iRODS path is from common links, i.e., in 'ResultsReports', 'MiscFiles', 'TrackHubs'.

        :param irods_path: iRODS path
        :type irods_path: str

        :return: Return True if path is in common links; otherwise, False.
        """
        common_links = {"ResultsReports", "MiscFiles", "TrackHubs"}
        path_part_set = set(irods_path.split("/"))
        return len(common_links.intersection(path_part_set)) > 0

    def get_assay_uuid(self, sodar_url, sodar_api_token, project_uuid):
        """Get assay UUID.

        :param sodar_url: SODAR url, e.g.: https://sodar.bihealth.org/
        :type sodar_url: str

        :param sodar_api_token: SODAR authentication token.
        :type sodar_api_token: str

        :param project_uuid: SODAR project UUID.
        :type project_uuid: str

        :return: Returns assay UUID.
        """
        investigation = api.samplesheet.retrieve(
            sodar_url=sodar_url, sodar_api_token=sodar_api_token, project_uuid=project_uuid
        )
        for study in investigation.studies.values():
            for _assay_uuid in study.assays:
                # If multi-assay project it will only consider the first one
                return _assay_uuid
        return None

    def get_irods_files(self, irods_local_path_pairs, force_overwrite=False):
        """Get iRODS files

        Retrieves iRODS path and stores it locally.

        :param irods_local_path_pairs: List of tuples (iRODS path [str], local output directory [str]).
        :type irods_local_path_pairs: list

        :param force_overwrite: Flag to indicate if local files should be overwritten.
        :type force_overwrite: bool
        """
        kw_options = {}
        if force_overwrite:
            kw_options = {FORCE_FLAG_KW: None}  # Keyword has no value, just needs to be present
        # Connect to iRODS
        with self._get_irods_sessions(count=1) as irods_sessions:
            try:
                for pair in irods_local_path_pairs:
                    # Set variable
                    file_name = pair[0].split("/")[-1]
                    irods_path = pair[0]
                    local_out_path = pair[1]
                    logger.info(f"Retrieving '{file_name}' from: {irods_path}")
                    # Create output directory if necessary
                    Path(local_out_path).parent.mkdir(parents=True, exist_ok=True)
                    # Get file
                    irods_sessions[0].data_objects.get(irods_path, local_out_path, **kw_options)

            except OVERWRITE_WITHOUT_FORCE_FLAG:
                logger.error(
                    f"Failed to retrieve '{file_name}', it already exists in output directory: {local_out_path}"
                )
                raise
            except Exception as e:
                logger.error(f"Failed to retrieve iRODS path: {irods_path}")
                logger.error(f"Attempted to copy file to directory: {local_out_path}")
                logger.error(self.get_irods_error(e))
                raise
