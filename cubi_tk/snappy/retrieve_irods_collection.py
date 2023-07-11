"""Contains classes and methods used to retrieve iRODS collections from SODAR.
"""
from collections import defaultdict
import typing

from irods.data_object import iRODSDataObject
from logzero import logger
from sodar_cli import api

from ..irods.check import IrodsCheckCommand

#: Default hash scheme. Although iRODS provides alternatives, the whole of `snappy` pipeline uses MD5.
DEFAULT_HASH_SCHEME = "MD5"


class RetrieveIrodsCollection(IrodsCheckCommand):
    """Class retrieves iRODS Collection associated with Assay"""

    def __init__(self, args, sodar_url, sodar_api_token, assay_uuid, project_uuid):
        """Constructor.

        :param sodar_url: SODAR url.
        :type sodar_url: str

        :param sodar_api_token: SODAR API token.
        :type sodar_api_token: str

        :param assay_uuid: Assay UUID.
        :type assay_uuid: str

        :param project_uuid: Project UUID.
        :type project_uuid: str
        """
        IrodsCheckCommand.__init__(self, args=args)
        self.sodar_url = sodar_url
        self.sodar_api_token = sodar_api_token
        self.assay_uuid = assay_uuid
        self.project_uuid = project_uuid

    def perform(self) -> typing.Dict[str, typing.List[iRODSDataObject]]:
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
            sodar_url=self.sodar_url,
            sodar_api_token=self.sodar_api_token,
            project_uuid=self.project_uuid,
        )
        for study in investigation.studies.values():
            if assay_uuid:
                logger.info(f"Using provided Assay UUID: {assay_uuid}")
                try:
                    assay = study.assays[assay_uuid]
                    return assay.irods_path
                except KeyError:
                    logger.error("Provided Assay UUID is not present in the Study.")
                    raise
            else:
                # Assumption: there is only one assay per study for `snappy` projects.
                # If multi-assay project it will only consider the first one and throw a warning.
                assays_ = list(study.assays.keys())
                if len(assays_) > 1:
                    self.multi_assay_warning(assays=assays_)
                for _assay_uuid in assays_:
                    assay = study.assays[_assay_uuid]
                    return assay.irods_path
        return None

    @staticmethod
    def multi_assay_warning(assays):
        """Display warning for multi-assay study.

        :param assays: Assays UUIDs as found in Studies.
        :type assays: list
        """
        multi_assay_str = "\n".join(assays)
        logger.warn(
            f"Project contains multiple Assays, will only consider UUID '{assays[0]}'.\n"
            f"All available UUIDs:\n{multi_assay_str}"
        )

    def retrieve_irods_data_objects(
        self, irods_path: str
    ) -> typing.Dict[str, typing.List[iRODSDataObject]]:
        """Retrieve data objects from iRODS.

        :param irods_path: iRODS path.

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of iRODSDataObject (native python-irodsclient object).
        """

        # Connect to iRODS
        with self._get_irods_sessions(1) as irods_sessions:
            try:
                root_coll = irods_sessions[0].collections.get(irods_path)

                # Get files and run checks
                logger.info("Querying for data objects")

                if root_coll is not None:
                    irods_data_objs = self.get_data_objs(root_coll)
                    irods_obj_dict = self.parse_irods_collection(irods_data_objs)
                    return irods_obj_dict

            except Exception as e:
                logger.error("Failed to retrieve iRODS path: %s", self.get_irods_error(e))
                raise

        return {}

    @staticmethod
    def parse_irods_collection(irods_data_objs) -> typing.Dict[str, typing.List[iRODSDataObject]]:
        """Parse iRODS collection

        :param irods_collection: iRODS collection.
        :type irods_collection: dict

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of iRODSDataObject (native python-irodsclient object).
        """
        # Initialise variables
        output_dict = defaultdict(list)

        for obj in irods_data_objs["files"]:
            output_dict[obj.name].append(obj)

        return output_dict
