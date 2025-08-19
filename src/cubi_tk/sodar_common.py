
import os
import time

from argparse import Namespace
from pathlib import Path
from typing import Dict, List

from irods.data_object import iRODSDataObject
from loguru import logger
from cubi_tk.sodar_api import SodarApi

from .common import is_uuid
from .exceptions import CubiTkException, ParameterException, UserCanceledException
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
        self.assay_uuid = self.sodar_api.assay_uuid

    def perform(self) -> Dict[str, List[iRODSDataObject]]:
        """Perform class routines."""
        logger.info("Starting remote files search ...")

        # Get assay iRODS path
        assay, _study = self.sodar_api.get_assay_from_uuid()
        self.assay_path = assay.irods_path
        self.assay_uuid = assay.sodar_uuid

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


class selectLandingzoneMixin:
    """Mixin to select the landing zone for iRODS transfers."""

    def __init__(self, argparse_args: Namespace, *args, **kwargs):
        # FIXME / NOTE:
        # - consider using a self.sodar_api and/or using the same approach as RetrieveSodarCollection
        # - this would allow to define self.project_uuid, self.lz_path, self.lz_uuid as @property
        self.args = argparse_args
        self._project_uuid = None
        self._lz_path = None
        self._lz_uuid = None
    def get_lz_info(self, sodar_api) -> tuple[str, str]:
        if self._lz_path is None or self._lz_uuid is None:
            # Update sodar info
            self._project_uuid, self._lz_uuid, self._lz_path = self._get_sodar_info(sodar_api)

        return self._lz_uuid, self._lz_path

    def get_project_uuid(self, sodar_api) -> str:
        """Get Project UUID.
        :return: Returns Project UUID.
        """
        if self._project_uuid is None:
            # Check if provided UUID is associated with a Project (we have access to)
            # FIXME: Suppress error in logging from sodar_api.get_samplesheet_retrieve()
            if is_uuid(self.args.destination) and sodar_api.get_samplesheet_retrieve() is not None:
                self._project_uuid = self.args.destination
            else:
                # Update sodar info
                self._project_uuid, self._lz_uuid, self._lz_path = self._get_sodar_info(sodar_api)

        return self._project_uuid


    def _get_sodar_info(self, sodar_api: SodarApi) -> (str, str, str):  #noqa: C901
        """Method evaluates user input to extract or create iRODS path. Use cases:

        1. User provides Landing Zone UUID: fetch path and use it.
        2. User provides Project UUID:
           i. If there are LZ associated with project, select the latest active and use it.
          ii. If there are no LZ associated with project, create a new one and use it.
        3. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.

        :return: (project_uuid, lz_uuid, lz_irods_path)
        """
        # Initialise variables
        lz_irods_path = None
        lz_uuid = None
        project_uuid = None

        # Check if desitination is project UUID provided by user
        # FIXME: Suppress error in logging from sodar_api.get_samplesheet_retrieve()
        if is_uuid(self.args.destination) and sodar_api.get_samplesheet_retrieve() is not None:
            project_uuid, lz_uuid, lz_irods_path = self._get_landing_zone(sodar_api, latest=not self.args.select_lz)
        # Provided UUID is NOT associated with a project, assume it is LZ instead
        elif is_uuid(self.args.destination):
            lz = sodar_api.get_landingzone_retrieve(self.args.destination)
            if lz:
                lz_irods_path = lz.irods_path
                lz_uuid = lz.sodar_uuid
                project_uuid = lz.project
            else:
                msg = "Provided UUID ({}) could neither be associated with a project nor with a Landing Zone.".format(
                    self.args.destination
                )
                raise ParameterException(msg)
        # Check if `destination` is a Landing zone (irods) path.
        elif self.args.destination.startswith("/"):
            project_uuid, lz_uuid, lz_irods_path = self._get_landing_zone(sodar_api, latest=not self.args.select_lz)
            if lz_uuid is None:
                msg = "Unable to identify UUID of given LZ {0}.".format(self.args.destination)
                raise ParameterException(msg)

        # Not able to process - raise exception.
        # UUID provided is not associated with project nor lz, or could not extract UUID from LZ path.
        if lz_irods_path is None:
            msg = "Data provided by user is not a valid UUID or LZ path. Please review input: {0}".format(
                self.args.destination
            )
            raise ParameterException(msg)

        # Log
        logger.info("Target iRODS path: {}", lz_irods_path)

        # Return
        return project_uuid, lz_uuid, lz_irods_path

    #possibly integrate these steps in Sodar/transfer specific class/function
    def _create_lz(self, sodar_api) -> (str, str, str):
        """
        Create a new landing zone (asking for user confirmation unless --yes is given) and check that is usable.
        :param sodar_api: sodar_api object from cubi-tk.sodar_api
        :return: (project_uuid, lz_uuid, lz_irods_path)
        """

        if self.args.yes or (
            input("Can the process create a new landing zone? [yN] ")
            .lower()
            .startswith("y")
        ):
            lz = sodar_api.post_landingzone_create()
            if lz:
                # check that async LZ creation task is done
                lz_usable = False
                while not lz_usable:
                    lz_check = sodar_api.get_landingzone_retrieve(lz.sodar_uuid)
                    if lz_check and lz_check.status == "ACTIVE":
                        lz_usable = True
                    logger.debug("Waiting 5 seconds for LZ {} to become usable...", lz.sodar_uuid)
                    time.sleep(5)  # wait and ask API again
                return lz.project, lz.sodar_uuid, lz.irods_path
            else:
                raise CubiTkException("Something went wrong during Lz creation")
        else:
            msg = "Not possible to continue the process without a landing zone path. Breaking..."
            raise UserCanceledException(msg)


    def _get_landing_zone(self, sodar_api, latest=True) -> (str, str, str):
        """
        Selection of landing zone to use for transfer. If --yes is given will use latest active landing zone
        or create a new one. With latest=False will ask user to select one of the available landing zones.
        :param sodar_api: sodar_api object from cubi-tk.sodar_api
        :param latest: boolean
        :return: (project_uuid, lz_uuid, lz_irods_path)
        """
        # Get existing LZs from API
        existing_lzs = sodar_api.get_landingzone_list(sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"])
        if not existing_lzs:
            # No active landing zones available
            logger.info("No active Landing Zone available.")
            return self._create_lz(sodar_api)

        logger.info(
            "Found {} active landing zone{}.".format(len(existing_lzs), 's' if len(existing_lzs) > 1 else "")
        )
        if not self.args.yes and not latest and (
            not input("Should the process use an existing landing zone? [yN] ")
            .lower()
            .startswith("y")
        ):
            return self._create_lz(sodar_api)

        if len(existing_lzs) == 1:
            lz = existing_lzs[0]
            logger.debug(f"Single active landingzone with UUID {lz.sodar_uuid} will be used")
        elif len(existing_lzs) > 1:
            if latest or self.args.yes:
                # Get latest active landing zone
                lz = existing_lzs[-1]
                logger.info(f"Latest active landingzone with UUID {lz.sodar_uuid} will be used")
            else:
                # Ask User which landing zone to use
                user_input = ""
                input_valid = False
                input_message = "####################\nPlease choose target landing zone:\n"
                for index, lz in enumerate(existing_lzs):
                    input_message += f"{index + 1}) {os.path.basename(lz.irods_path)} ({lz.sodar_uuid})\n"
                input_message += "Select by number: "

                while not input_valid:
                    user_input = input(input_message)
                    if user_input.isdigit():
                        user_input = int(user_input)
                        if 0 < user_input <= len(existing_lzs):
                            input_valid = True

                lz = existing_lzs[user_input - 1]

        # Return
        return lz.project, lz.sodar_uuid, lz.irods_path
