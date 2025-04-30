from argparse import Namespace
from pathlib import Path
from typing import Dict, List

from irods.data_object import iRODSDataObject
from loguru import logger
from cubi_tk.sodar_api import SodarApi


from .irods_common import iRODSRetrieveCollection


class RetrieveSodarCollection(iRODSRetrieveCollection):
    def __init__(
        self,
        args: Namespace,
        irods_env_path: Path = None,
    ):
        """Constructor.
        :param sodar_server_url: SODAR url.
        :type sodar_server_url: str

        :param sodar_api_token: SODAR API token.
        :type sodar_api_token: str

        :param assay_uuid: Assay UUID.
        :type assay_uuid: str

        :param project_uuid: Project UUID.
        :type project_uuid: str

        :param ask: Confirm with user before certain actions.
        :type ask: bool, optional

        :param irods_env_path: Path to irods_environment.json
        :type irods_env_path: pathlib.Path, optional
        """
        super().__init__(ask= getattr(args, "yes", False), irods_env_path= irods_env_path, sodar_profile = getattr(args, "config_profile", "global"))
        self.sodar_api = SodarApi(args, with_dest=True)
        self.assay_path = None

    def perform(self) -> Dict[str, List[iRODSDataObject]]:
        """Perform class routines."""
        logger.info("Starting remote files search ...")

        # Get assay iRODS path
        assay, _study = self.sodar_api.get_assay_from_uuid()
        self.assay_path = assay.irods_path

        # Get iRODS collection
        irods_collection_dict = {}
        if self.assay_path:
            irods_collection_dict = self.retrieve_irods_data_objects(irods_path=self.assay_path)

        logger.info("... done with remote files search.")
        return irods_collection_dict

    def get_assay_uuid(self):
        return self.assay_uuid


    def get_assay_irods_path(self):
        """Get Assay iRODS path.

        :param assay_uuid: Assay UUID.
        :type assay_uuid: str [optional]

        :return: Returns Assay iRODS path - extracted via SODAR API.
        """
        return self.assay_path


