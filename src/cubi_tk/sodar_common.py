
import os

from argparse import Namespace
from pathlib import Path
from typing import Dict, List

from irods.data_object import iRODSDataObject
from loguru import logger
from cubi_tk.sodar_api import SodarApi

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


class SodarIngestBase:
    """Base class for iRODS transfers to Sodar.
    Includes methods for proper (study, assay &) landing zone selection/creation.
    Should always be used with argparse base parser `get_sodar_parser(with_dest=True, with_assay_uuid=True, dest_string="destination")
    """
    def __init__(self, args: Namespace):
        self.args = args
        self.select_lz = getattr(args, "select_lz", True)
        self.sodar_api = SodarApi(args, with_dest=True, dest_string="destination")
        self.lz_uuid, self.lz_irods_path = self._get_lz_info()


    def get_lz_info(self) -> tuple[str, str]:
        """Method evaluates user input to extract or create iRODS path. Use cases:

        1. User provide LZ path (set in SodarAPI as lz_path): fetch lz uuid
        2. User provides UUID (set in SodarAPI as project_uuid):
            i.UUID is LZ UUID: fetch path and use it.
            ii.UUID is Project UUID: get or create LZs
        3. Data provided by user is neither an iRODS path nor a valid UUID. Report error and throw exception.

        :return: (project_uuid, lz_uuid, lz_irods_path)
        """
        #lz path given, projectuuid set up, lz set up, check if valid and get lz_uuid
        if self.sodar_api.lz_path is not None:
            lz_path = self.sodar_api.lz_path
            existing_lzs = self.sodar_api.get_landingzone_list(sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"])
            if existing_lzs is not None and len(existing_lzs) == 1: #lz exists
                lz_uuid = existing_lzs[0].sodar_uuid
            else:
                msg = "Unable to identify UUID of given LZ Path{0}.".format(self.sodar_api.lz_path)
                raise ParameterException(msg)
        #either projectuuid or lz uuid
        elif self.sodar_api.project_uuid is not None:
            lz = self.sodar_api.get_landingzone_retrieve(log_error=False)
            # if succees given uuid is lz, everything set up, sodarapi will set project uui and lz path
            if lz is not None:
                lz_uuid = lz.sodar_uuid
                lz_path = lz.irods_path
            #if None: projectuuid is possibly given
            #check if projectuuid is valid and start lz selection
            elif self.sodar_api.get_samplesheet_retrieve(log_error=False) is not None:
                try:
                   lz_uuid, lz_path = self._get_landing_zone(latest=not self.select_lz)
                except UserCanceledException as e:
                    raise e
            #neither project nor lz uuid
            else:
                msg = "Provided UUID ({}) could neither be associated with a project nor with a Landing Zone.".format(
                    self.sodar_api.project_uuid
                )
                raise ParameterException(msg)
        #invalid input
        else:
            msg = "Data provided by user is not a valid UUID or LZ path. Please review input: {0}".format(
                self.args.destination
            )
            raise ParameterException(msg)
        # Log
        logger.info("Target iRODS path: {}", lz_path)
        return lz_uuid, lz_path

    def _create_lz(self) -> tuple[str, str]:
        """
        Create a new landing zone (asking for user confirmation unless --yes is given) and check that is usable.
        :param sodar_api: sodar_api object from cubi-tk.sodar_api
        :return: (project_uuid, lz_uuid, lz_irods_path)
        """
        if self.sodar_api.yes or (
            input("Can the process create a new landing zone? [yN] ").lower().startswith("y")
        ):
            lz = self.sodar_api.post_landingzone_create(wait_until_ready = True)
            if lz:
                return lz.sodar_uuid, lz.irods_path
            else:
                raise CubiTkException("Something went wrong during Lz creation")
        else:
            msg = "Not possible to continue the process without a landing zone path. Breaking..."
            raise UserCanceledException(msg)

    def _get_landing_zone(self, latest=True) -> tuple[str, str]:
        """
        Selection of landing zone to use for transfer. If --yes is given will use latest active landing zone
        or create a new one. With latest=False will ask user to select one of the available landing zones.
        :param sodar_api: sodar_api object from cubi-tk.sodar_api
        :param latest: boolean
        :return: (project_uuid, lz_uuid, lz_irods_path)
        """
        # Get existing LZs from API
        existing_lzs = self.sodar_api.get_landingzone_list(sort_reverse=True, filter_for_state=["ACTIVE", "FAILED"])
        if not existing_lzs:
            # No active landing zones available
            logger.info("No active Landing Zone available.")
            return self._create_lz()
        logger.info(
            "Found {} active landing zone{}.".format(len(existing_lzs), 's' if len(existing_lzs) > 1 else "")
        )
        if not self.sodar_api.yes and not latest and (
            not input("Should the process use an existing landing zone? [yN] ")
            .lower()
            .startswith("y")
        ):
            return self._create_lz()
        if len(existing_lzs) == 1:
            lz = existing_lzs[0]
            logger.debug(f"Single active landingzone with UUID {lz.sodar_uuid} will be used")
        elif len(existing_lzs) > 1:
            if latest or self.sodar_api.yes:
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
        return lz.sodar_uuid, lz.irods_path
