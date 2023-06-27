"""Contains classes and methods used to retrieve iRODS collections from SODAR.
"""
from itertools import chain
from collections import defaultdict
import re

import attr
from logzero import logger
from sodar_cli import api

from ..irods.check import HASH_SCHEMES, IrodsCheckCommand

from irods.models import DataObject, Collection
from irods.column import In, Like, Criterion

#: Default hash scheme. Although iRODS provides alternatives, the whole of `snappy` pipeline uses MD5.
DEFAULT_HASH_SCHEME = "MD5"


@attr.s(frozen=True, auto_attribs=True)
class IrodsDataObject:
    """iRODS data object - simplified version of data provided in iRODS Collections."""

    file_name: str
    irods_path: str
    file_md5sum: str
    replicas_md5sum: list


@attr.s(frozen=True, auto_attribs=True)
class IrodsRawDataObject:
    name: str
    path: str
    manager: object

    def open(self, mode='r', finalize_on_close = True, **options):
        return self.manager.open(self.path, mode, finalize_on_close = finalize_on_close, **options)


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

    def perform(self, identifiers=None, extensions_tuple=None):
        """Perform class routines.

        :return: Returns iRODS collection represented as dictionary: key: file name as string (e.g.,
        'P001-N1-DNA1-WES1'); value: iRODS data (``IrodsDataObject``).
        """
        logger.info("Starting remote files search ...")

        # Get assay iRODS path
        assay_path = self.get_assay_irods_path(assay_uuid=self.assay_uuid)

        # Get iRODS collection
        irods_collection_dict = self.retrieve_irods_data_objects(irods_path=assay_path, identifiers=identifiers, extensions_tuple=extensions_tuple)

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

    def retrieve_irods_data_objects(self, irods_path, identifiers=None, extensions_tuple=None):
        """Retrieve data objects from iRODS.

        :param irods_path: iRODS path.
        :type irods_path: str

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of IrodsDataObject (attributes: 'file_name', 'irods_path', 'file_md5sum', 'replicas_md5sum').
        """

        # Connect to iRODS
        with self._get_irods_sessions(1) as irods_sessions:
            data_obj_manager = irods_sessions[0].data_objects
            try:
                results = irods_sessions[0].query(Collection.name, DataObject.name, DataObject.path).filter(Like(Collection.name, f"{irods_path}%")).filter(Criterion("not like", DataObject.name, f"%.{DEFAULT_HASH_SCHEME.lower()}"))

                checksum_results = irods_sessions[0].query(Collection.name, DataObject.name, DataObject.path).filter(Like(Collection.name, f"{irods_path}%")).filter(Criterion("like", DataObject.name, f"%.{DEFAULT_HASH_SCHEME.lower()}"))
            except Exception as e:
                logger.error("Failed to retrieve iRODS path: %s", self.get_irods_error(e))
                raise

            # Get files and run checks
            logger.info("Querying for data objects")
            # irods_collection = self.get_data_objs(root_coll)
            irods_collection = {
                "files": [IrodsRawDataObject(name=r[DataObject.name], path=f"{r[Collection.name]}/{r[DataObject.name]}", manager=data_obj_manager) for r in results],
                "checksums": {
                    f"{r[Collection.name]}/{r[DataObject.name]}": IrodsRawDataObject(name=r[DataObject.name], path=f"{r[Collection.name]}/{r[DataObject.name]}", manager=data_obj_manager)
                    for r in checksum_results
                }
            }

            if extensions_tuple and identifiers:
                irods_collection["files"] = [
                    f for f in irods_collection["files"]
                    if f.path.endswith(extensions_tuple) and any(id in f.path for id in identifiers)
                ]

            return self.parse_irods_collection(irods_collection=irods_collection)

    @staticmethod
    def parse_irods_collection(irods_collection):
        """Parse iRODS collection

        :param irods_collection: iRODS collection.
        :type irods_collection: dict

        :return: Returns dictionary representation of iRODS collection information. Key: File name in iRODS (str);
        Value: list of IrodsDataObject (attributes: 'file_name', 'irods_path', 'file_md5sum', 'replicas_md5sum').
        """
        # Initialise variables
        output_dict = defaultdict(list)
        checksums = irods_collection["checksums"]

        # Extract relevant info from iRODS collection: file and replicates MD5SUM
        for data_obj in irods_collection["files"]:
            chk_obj = checksums.get(data_obj.path + "." + DEFAULT_HASH_SCHEME.lower())
            if not chk_obj:
                logger.error(f"No checksum file for: {data_obj.path}")
                continue
            with chk_obj.open("r") as f:
                file_sum = re.search(
                    HASH_SCHEMES[DEFAULT_HASH_SCHEME]["regex"], f.read().decode("utf-8")
                ).group(0)
                output_dict[data_obj.name].append(
                    IrodsDataObject(
                        file_name=data_obj.name,
                        irods_path=data_obj.path,
                        file_md5sum=file_sum,
                        replicas_md5sum=[],
                        # replicas_md5sum=[replica.checksum for replica in data_obj.replicas],
                    )
                )
        return output_dict
