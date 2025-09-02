from argparse import Namespace
from collections import defaultdict

from cubi_tk.api_models import iRODSDataObject as iRODSDataObject
from cubi_tk.irods_common import iRODSCommon
from cubi_tk.sodar_api import SodarApi


# API based drop-in replacement for what used to build on the `iRODSRetrieveCollection` class (to be deprecated)
class RetrieveSodarCollection(SodarApi):

    def __init__(self, argparse: Namespace, **kwargs):
        super().__init__(argparse, **kwargs)
        irods_hash_scheme = iRODSCommon(sodar_profile=argparse.config_profile).irods_hash_scheme()
        self.hash_ending = "." + irods_hash_scheme.lower()

    def perform(self, include_hash_files=False) -> dict[str, list[iRODSDataObject]]:

        filelist = self.get_samplesheet_file_list()

        output_dict = defaultdict(list)

        for obj in filelist:
            if obj.type == 'obj' and obj.name.endswith(self.hash_ending) and not include_hash_files:
                continue
            output_dict[obj.name].append(obj)

        return output_dict

    def get_assay_uuid(self):
        if self.assay_uuid:
            return self.assay_uuid

        assay, _ = self.get_assay_from_uuid()
        return assay.sodar_uuid

    def get_assay_irods_path(self):
        assay, _ = self.get_assay_from_uuid()
        return assay.irods_path
