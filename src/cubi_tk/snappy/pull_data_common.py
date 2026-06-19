from datetime import datetime
import re

from loguru import logger

from ..sodar_common import SodarPullBase
from .common import get_biomedsheet_path, load_sheet_tsv

#: Valid file extensions
VALID_FILE_TYPES = ("bam", "vcf", "txt", "csv", "log")


class SnappyPullBase(SodarPullBase):
    """Implementation of common pull data methods."""

    #: File type dictionary. Key: file type; Value: additional expected extensions (tuple).
    file_type_to_extensions_dict = None

    def get_output_basepath(self):
        return self.args.output_dir

    def get_output_filepath(self, out_parts: dict[str, str]):
        # apply regexes
        for filepart, m_pat, r_pat in self.args.output_regex:
            out_parts[filepart] = re.sub(m_pat, r_pat, out_parts[filepart])
        return self.args.output_pattern.format(**out_parts)

    def get_sample_list(self) -> set[str] | None:

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


        return samples

    def get_file_patterns(self) -> list[str]:
        """Function to get samples to filter downloadable files by collection"""
        if self.args.all_files:
            file_patterns = []
        elif self.args.preset:
            file_patterns = self.presets[self.args.preset]
        else:  # self.args.file_pattern
            file_patterns = self.args.file_pattern
        return file_patterns

    def get_substring_match(self) -> bool:
        return False


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
