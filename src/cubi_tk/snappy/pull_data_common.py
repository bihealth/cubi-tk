from datetime import datetime
from typing import Dict, List
import warnings

from irods.data_object import iRODSDataObject
from loguru import logger

from ..irods_common import TransferJob, iRODSTransfer

#: Valid file extensions
VALID_FILE_TYPES = ("bam", "vcf", "txt", "csv", "log")


# FIXME: this is not used by the new `pull-data` command and should also be deprecated
class PullDataCommon:
    """Implementation of common pull data methods."""

    #: File type dictionary. Key: file type; Value: additional expected extensions (tuple).
    file_type_to_extensions_dict = None

    def __init__(self):
        warnings.warn(
            "The `PullDataCommon` class will be deprecated",
            DeprecationWarning, stacklevel=2
        )
        pass

    def filter_irods_collection(
        self,
        identifiers: List[str],
        remote_files_dict: Dict[str, List[iRODSDataObject]],
        file_type: str,
    ) -> Dict[str, List[iRODSDataObject]]:
        """Filter iRODS collection based on identifiers (sample id or library name) and file type/extension.

        :param identifiers: List of sample identifiers or library names.
        :type identifiers: list

        :param remote_files_dict: Dictionary with iRODS collection information. Key: file name as string (e.g.,
        'P001-N1-DNA1-WES1.vcf.gz'); Value: list of iRODS data (``iRODSDataObject``).
        :type remote_files_dict: dict

        :param file_type: File type, example: 'bam' or 'vcf'.
        :type file_type: str

        :return: Returns dictionary: Key: identifier (sample name [str]); Value: list of iRODS objects.
        """
        # Initialise variables
        filtered_dict = {}
        extensions_tuple = self.file_type_to_extensions_dict.get(file_type)

        # Iterate
        for key, value in remote_files_dict.items():
            # Simplify criteria: must have the correct file extension
            if not key.endswith(extensions_tuple):
                continue

            # Check for common links
            # Note: if a file with the same name is present in both assay and in a common file, it will be ignored.
            in_common_links = False
            for irods_obj in value:
                in_common_links = self._irods_path_in_common_links(irods_obj.path)
                if in_common_links:
                    break

            # Filter
            if (
                any(id_ in key for id_ in identifiers)  # presence of identifiers
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

    @staticmethod
    def get_irods_files(irods_local_path_pairs, force_overwrite=False, sodar_profile = "global"):
        """Get iRODS files

        Retrieves iRODS path and stores it locally.

        :param irods_local_path_pairs: List of tuples (iRODS path [str], local output directory [str]).
        :type irods_local_path_pairs: list

        :param force_overwrite: Flag to indicate if local files should be overwritten.
        :type force_overwrite: bool
        """

        transfer_jobs = [
            TransferJob(local_out_path, irods_path)
            for irods_path, local_out_path in irods_local_path_pairs
        ]
        iRODSTransfer(transfer_jobs).get(force_overwrite)

    @staticmethod
    def report_no_file_found(available_files):
        """Report no files found

        :param available_files: List of available files in SODAR.
        :type available_files: list
        """
        available_files = sorted(available_files)
        if len(available_files) > 50:
            limited_str = " (limited to first 50)"
            ellipsis_ = "..."
            remote_files_str = "\n".join(available_files[:50])
        else:
            limited_str = ""
            ellipsis_ = ""
            remote_files_str = "\n".join(available_files)
        logger.warning(
            f"No file was found using the selected criteria.\n"
            f"Available files{limited_str}:\n{remote_files_str}\n{ellipsis_}"
        )

    def sort_irods_object_by_date_in_path(self, irods_obj_list):
        """Sort list of iRODS object: latest to earliest.

        Sort by date as defined in path, hence the main assumption is that there is a date somewhere in iRODS path:
        /sodarZone/projects/../<PROJECT_UUID>/.../assay_<ASSAY_UUID>/<LIBRARY_NAME>/.../<DATE>/...

        :param irods_obj_list: List of iRODS objects derived from collection in SODAR.
        :type irods_obj_list: List[iRODSDataObject]

        :return: Returns inputted list sorted from latest to earliest iRODS object.
        """
        if not irods_obj_list:
            logger.warning("Provided list doesn't contain any iRODS objects.")
            return irods_obj_list
        return sorted(
            irods_obj_list,
            key=lambda irods_obj: self._find_date_in_path(irods_obj.path),
            reverse=True,
        )

    @staticmethod
    def _find_date_in_path(path):
        """Find date in provided path.

        If multiple dates found in path, it will return the first one, i.e., closer to the root.
        The accepted date formats are the following:
            '%Y-%m-%d'
            '%Y_%m_%d'
            '%Y%m%d'

        :param path: iRODS path.
        :type path: str

        :return: Returns ``<class 'datetime.datetime'>`` based on directory name.
        """
        accepted_date_format = ("%Y-%m-%d", "%Y_%m_%d", "%Y%m%d")
        for dir_ in path.split("/"):
            for date_format in accepted_date_format:
                try:
                    date = datetime.strptime(dir_, date_format)
                    return date
                except ValueError:
                    pass
        accepted_date_format_str = ", ".join(accepted_date_format)
        raise ValueError(
            f"Could not find a valid date in path: {path}\nTested date formats: {accepted_date_format_str}."
        )
