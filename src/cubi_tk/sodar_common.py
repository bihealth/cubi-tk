from pathlib import Path
from typing import Dict, List

from irods.data_object import iRODSDataObject
from loguru import logger
from sodar_cli import api

from .irods_common import DEFAULT_HASH_SCHEME, iRODSRetrieveCollection


class RetrieveSodarCollection(iRODSRetrieveCollection):
    def __init__(
        self,
        sodar_server_url,
        sodar_api_token,
        assay_uuid,
        project_uuid,
        hash_scheme: str = DEFAULT_HASH_SCHEME,
        ask: bool = False,
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

        :param hash_scheme: iRODS hash scheme, default MD5.
        :type hash_scheme: str, optional

        :param ask: Confirm with user before certain actions.
        :type ask: bool, optional

        :param irods_env_path: Path to irods_environment.json
        :type irods_env_path: pathlib.Path, optional
        """
        super().__init__(hash_scheme, ask, irods_env_path)
        self.sodar_server_url = sodar_server_url
        self.sodar_api_token = sodar_api_token
        self.assay_uuid = assay_uuid
        self.project_uuid = project_uuid

    def perform(self) -> Dict[str, List[iRODSDataObject]]:
        """Perform class routines."""
        logger.info("Starting remote files search ...")

        # Get assay iRODS path
        assay_path = self.get_assay_irods_path(assay_uuid=self.assay_uuid)

        # Get iRODS collection
        irods_collection_dict = {}
        if assay_path:
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
            sodar_url=self.sodar_server_url,
            sodar_api_token=self.sodar_api_token,
            project_uuid=self.project_uuid,
        )

        for study in investigation.studies.values():
            if assay_uuid:
                #bug fix for rare case that multiple studies and multiple assays exist
                if assay_uuid in study.assays.keys():
                    logger.info(f"Using provided Assay UUID: {assay_uuid}")
                    assay = study.assays[assay_uuid]
                    return assay.irods_path

            else:
                # Assumption: there is only one assay per study for `snappy` projects.
                # If multi-assay project it will only consider the first one and throw a warning.
                assays_ = list(study.assays.keys())
                if len(assays_) > 1:
                    self.multi_assay_warning(assays=assays_)
                for _assay_uuid in assays_:
                    assay = study.assays[_assay_uuid]
                    return assay.irods_path

        if assay_uuid:
            logger.error("Provided Assay UUID is not present in the Investigation.")
            raise Exception("Cannot find assay with UUID %s" % assay_uuid)
        return None

    @staticmethod
    def multi_assay_warning(assays):
        """Display warning for multi-assay study.

        :param assays: Assays UUIDs as found in Studies.
        :type assays: list
        """
        multi_assay_str = "\n".join(assays)
        logger.warning(
            f"Project contains multiple Assays, will only consider UUID '{assays[0]}'.\n"
            f"All available UUIDs:\n{multi_assay_str}"
        )
