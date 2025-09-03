import argparse
from functools import reduce
import sys
from typing import List, Literal
import urllib.parse as urlparse
from uuid import UUID

import cattr
from loguru import logger
import requests

from cubi_tk.common import is_uuid
from cubi_tk import api_models

import toml
import os
from .exceptions import ParameterException, SodarApiException

#: Paths to search the global configuration in.
GLOBAL_CONFIG_PATH = "~/.cubitkrc.toml"

def get_user_input_assay_study(assay_study_uuids: list[str], assays_studies:dict[str, api_models.Study|api_models.Assay],string :str = "Assays") -> UUID:
    """Display available assay or study UUIDS and let User choose which one to use.
    """
    logger.warning(f"Multiple {string} present, which one do you want to choose?")
    for i, content_uuid in enumerate(assay_study_uuids):
        content_name = assays_studies[content_uuid].file_name
        logger.warning("{}: {}", i+1, content_name)
    content_num = 0
    while (content_num<= 0 or content_num> len(assay_study_uuids)):
        content_num = input("Please enter the index of the Assay/Study (e.g 2):")
        content_num =int(content_num)
    content_uuid = assay_study_uuids[content_num-1]
    logger.info("Chosen Assay/Study: {}", assays_studies[content_uuid].file_name)
    return content_uuid

def multi_assay_study_warning(content:dict, string :str = "Assays") -> None:
    """Display warning for multi-assays or studies.
    """
    multi_assay_str = "\n".join(content)
    logger.warning(
        f"Project contains multiple {string}, will only consider UUID '{content[0]}'.\n"
        f"All available UUIDs:\n{multi_assay_str}"
    )


SODAR_API_VERSION_SAMPLESHEETS = 1.1
SODAR_API_VERSION_LANDINGZONES = 1.0

LANDING_ZONE_STATES = ["ACTIVE", "FAILED", "VALIDATING"]


class SodarApi:
    """
    params:
    args: parsed input arguments (see parsers.py get_sodar_parser())
    if set_default is true, default values will be set, otherwise an error will be thrown if required params are missing (serverurl and api token)
    if with_dest is true the destination will be checked and sodarapi set up accordingly, can be project_uuid, destination (project_uuid, lz_path or lz_uuid) or landing_zone_uuid
    """
    def __init__(self, args: argparse.Namespace, set_default: bool = False, with_dest: bool = False, dest_string: str = "project_uuid"):
        any_error, args = self.setup_sodar_params(args, set_default=set_default, with_dest=with_dest, dest_string=dest_string)
        if any_error:
            sys.exit(1)
        self.sodar_server_url = args.sodar_server_url
        self.project_uuid = args.project_uuid
        self.assay_uuid = getattr(args, "assay_uuid", None)
        self.lz_path = getattr(args, "destination", None) #if destiantion exists and destination is lz path (!= project_uuid), set lz_path
        if self.lz_path == self.project_uuid:
            self.lz_path = None
        self.yes = getattr(args, "yes", False)
        self.sodar_headers = {
            "samplesheets": {
                "Authorization": "token {}".format(args.sodar_api_token),
                'Accept': f'application/vnd.bihealth.sodar.samplesheets+json; version={SODAR_API_VERSION_SAMPLESHEETS}'
            },
            "landingzones": {
                "Authorization": "token {}".format(args.sodar_api_token),
                'Accept': f'application/vnd.bihealth.sodar.landingzones+json; version={SODAR_API_VERSION_LANDINGZONES}'
            },
        }

    def _api_call(
        self,
        api: Literal["samplesheets", "landingzones"],
        action: str,
        method: Literal["get", "post"] = "get",
        params: dict = None,
        data: dict = None,
        files: dict = None,
        dest_uuid: UUID = None
    ) -> dict:
        # need to add trailing slashes to all parts of the URL for urljoin to work correctly
        # afterward remove the final trailing slash from the joined URL
        if dest_uuid is None:
            dest_uuid = self.project_uuid
        base_url_parts = [
            part if part.endswith("/") else f"{part}/"
            for part in (self.sodar_server_url, api, "api", action, dest_uuid)
        ]
        url = reduce(urlparse.urljoin, base_url_parts)[:-1]
        if params and method == "get":
            url += "?" + urlparse.urlencode(params)
        elif params and method == "post":
            # For POST requests, params are sent in the body, not as query parameters
            data = data or {}
            data.update(params)

        if method == "get":
            logger.debug(f"HTTP GET request to {url} with headers {self.sodar_headers[api]}")
            response = requests.get(url, headers=self.sodar_headers[api])
        elif method == "post":
            logger.debug(f"HTTP POST request to {url} with headers {self.sodar_headers[api]}, files {files}, and data {data}")
            response = requests.post(url, headers=self.sodar_headers[api], files=files, data=data)
        else:
            raise ValueError("Unknown HTTP method.")

        if response.status_code != 200 and response.status_code != 201:
            raise SodarApiException(response.status_code, f"API response: {response.text} and status code: {response.status_code}")

        return response.json()

    # Samplesheet api calls
    def get_samplesheet_export(self, get_all: bool = False) -> dict[str, dict] | None:
        logger.debug("Exporting samplesheet..")
        try:
            samplesheet = self._api_call("samplesheets", "export/json")
        except SodarApiException as e:
            logger.error(f"Failed to export samplesheet:\n{e}")
            return None
        if get_all:
            logger.debug("Returning all samplesheet infos : {}", samplesheet)
            return samplesheet

        study_name = list(samplesheet["studies"].keys())[0]
        assay_name = list(samplesheet["assays"].keys())[0]
        if len(samplesheet["studies"]) > 1 or len(samplesheet["assays"]) > 1:
            assay, study = self.get_assay_from_uuid()
            assay_name = assay.file_name
            study_name= study.file_name

        small_samplesheet={
            "investigation": {
                "path": samplesheet["investigation"]["path"],
                "tsv": samplesheet["investigation"]["tsv"],
            },
            "studies": {study_name : samplesheet["studies"][study_name]},
            "assays": {assay_name : samplesheet["assays"][assay_name]},
        }
        logger.debug("Returning all samplesheet with single assay and study : {}", small_samplesheet)
        return small_samplesheet

    def get_samplesheet_investigation_retrieve(self) -> api_models.Investigation | None:
        logger.debug("Get investigation information.")
        try:
            investigationJson = self._api_call("samplesheets", "investigation/retrieve")
            investigation = cattr.structure(investigationJson, api_models.Investigation)
        except SodarApiException as e:
            logger.error(f"Failed to retrieve investigation information:\n{e}")
            return None
        logger.debug(f"Got investigation: {investigation}")
        return investigation

    #Todo: remove/refactor write_sampleinfo sea snap
    def get_samplesheet_remote(self) -> dict | None:
        logger.debug("Get remote samplesheet isa information.")
        #valid Uri?
        try:
            samplesheet = self._api_call("samplesheets", "remote/get", params={"isa" : 1})
        except SodarApiException as e:
            logger.error(f"Failed to retrieve samplesheets information:\n{e}")
            return None
        logger.debug(f"Got samplesheet: {samplesheet}")
        return samplesheet

    def get_samplesheet_file_list(self) -> list[api_models.iRODSDataObject]:
        logger.debug("Getting irods file list")
        try:
            json_filelist = self._api_call("samplesheets", "file/list")
        except SodarApiException as e:
            logger.error(f"Failed to retrieve Sodar file list:\n{e}")
            return None

        filelist = [cattr.structure(obj, api_models.iRODSDataObject) for obj in json_filelist]

        return filelist


    def post_samplesheet_import(
        self,
        files_dict: dict[str, tuple[str, str]],
    ) -> int:
        logger.debug("Posting samplesheet..")
        for key, value in files_dict.items():
            files_dict[key] = (*value, "text/plain")
        try:
            ret_val = self._api_call(
                "samplesheets",
                "import",
                method="post",
                files=files_dict,
            )
            if "sodar_warnings" in ret_val:
                logger.info("ISA-tab uploaded with warnings.")
                for warning in ret_val["sodar_warnings"]:
                    logger.warning(f"SODAR warning: {warning}")
            else:
                logger.info("ISA-tab uploaded successfully.")
            return 0
        except SodarApiException as e:
            logger.error(f"Failed to upload ISA-tab:\n{e}")
            return 1

    def post_samplesheet_deletion_request_create(self, path, description=None) -> int:
        params = {'path': path}
        if description:
            params.update({'description': description})

        try:
            ret_val = self._api_call(
                "samplesheets",
                "irods/request/create",
                method="post",
                params=params,
            )
            if "sodar_warnings" in ret_val:
                for warning in ret_val["sodar_warnings"]:
                    logger.warning(f"SODAR warning: {warning}")
            else:
                logger.info(f"Sodar deletion request created successfully: {path}.")
            return 0
        except SodarApiException as e:
            logger.error(f"Failed to create Sodar deletion request:\n{e}")
            return 1


    # landingzone Api calls
    def get_landingzone_retrieve(self, lz_uuid: UUID = None) -> api_models.LandingZone | None:
        logger.debug("Retrieving Landing Zone ...")
        try:
            landingzone = self._api_call("landingzones", "retrieve", dest_uuid=lz_uuid) #if None: assume projectuuid is lz_uuid
            landingzone = cattr.structure(landingzone, api_models.LandingZone)
            self.project_uuid = landingzone.project
            if not self.assay_uuid:
                self.assay_uuid = landingzone.assay
            return landingzone
        except SodarApiException as e:
            logger.error(f"Failed to retrieve Landingzone:\n{e}")
            return None

    # maybe use status_locked of lz and only retrun not locked if wanted (instead of filter_for_state filter_for_not_locked:bool)
    def get_landingzone_list(self, sort_reverse:bool = False, filter_for_state :list[str]=LANDING_ZONE_STATES) -> List[api_models.LandingZone]|None:
        logger.debug("Get list of Landing Zones...")
        try:
            landingzones_json = self._api_call("landingzones", "list")
            landingzones = cattr.structure(landingzones_json, List[api_models.LandingZone])
        except SodarApiException as e:
            logger.error(f"Failed to retrieve Landingzone:\n{e}")
            return None
        landingzones = sorted(
                landingzones,
                key=lambda lz: lz.date_modified,
                reverse=sort_reverse
            )
        #if assay_uuid filter for assay_uuid
        if self.assay_uuid:
            landingzones = list(filter(lambda lz: lz.assay == self.assay_uuid, landingzones))
        #if lz path filter for irods path
        if self.lz_path is not None:
            landingzones = list(filter(lambda lz: lz.irods_path == self.lz_path, landingzones))
        # Get the lzs with allowed state
        landingzones = list(filter(lambda lz: lz.status in filter_for_state, landingzones))

        return landingzones

    def post_landingzone_create(self, create_colls: bool = True, restrict_colls: bool = True) -> api_models.LandingZone | None:
        logger.debug("Creating new Landing Zone...")
        if not self.assay_uuid:
            self.get_assay_from_uuid()
        try:
            ret_val = self._api_call("landingzones", "create", method="post",
                params={"create_colls" : create_colls, "restrict_colls": restrict_colls},
                data={"assay": self.assay_uuid},
            )
            if "sodar_warnings" in ret_val:
                logger.info("Landingzone created with warnings.")
                for warning in ret_val["sodar_warnings"]:
                    logger.warning(f"SODAR warning: {warning}")
            else:
                logger.info("Landingzone created successfully.")
            return cattr.structure(ret_val, api_models.LandingZone)
        except SodarApiException as e:
            if e.status_code == 503:
                logger.error("Investigation for the project is not found or project iRODS collections have not been created")
            logger.error(f"Failed to create Landingzone:\n{e}")
            return None


    def post_landingzone_submit_move(self, lz_uuid : UUID)-> UUID | None:
        logger.debug("Moving landing zone with the given UUID")
        try:
            ret_val = self._api_call("landingzones", "submit/move", method="post", dest_uuid=lz_uuid)
            new_uuid = ret_val["sodar_uuid"]
            logger.info("Landingzone with UUID {} moved successfully.", new_uuid)
            return new_uuid
        except SodarApiException as e:
            if e.status_code == 503:
                logger.error("Project is currently locked by another operation")
            logger.error(f"Failed to move Landingzone:\n{e}")
            return None

    def post_landingzone_submit_validate(self, lz_uuid: UUID)-> UUID | None:
        logger.debug("Validating landing zone with the given UUID")
        try:
            ret_val = self._api_call("landingzones", "submit/validate", method="post", dest_uuid=lz_uuid)
            new_uuid = ret_val["sodar_uuid"]
            logger.info("Landingzone with UUID {} valiated successfully.", new_uuid)
            return new_uuid
        except SodarApiException as e:
            if e.status_code == 503:
                logger.error("Project is currently locked by another operation")
            logger.error(f"Failed to validate Landingzone:\n{e}")
            return None

    # helper functions
    def get_assay_from_uuid(self)-> (tuple[None, None] | tuple[api_models.Assay, api_models.Study]):
        investigation = self.get_samplesheet_investigation_retrieve()
        if investigation is None:
            return None, None
        studies = investigation.studies.values()
        # if assay_uuid given and multiple studies iterate through all studies and find assay
        # if mulitple studies and yes, iterate through first study
        # if multiple studies, no asssay uuid and not yes, let user decide which study to use
        if len(studies) > 1 and not self.assay_uuid:
            study_keys = list(investigation.studies.keys())
            if not self.yes:
                study_uuid = get_user_input_assay_study(study_keys, investigation.studies, string="studies")
                studies = [investigation.studies[study_uuid]]
            else:
                multi_assay_study_warning(study_keys, string="studies")

        for study in studies:
            if self.assay_uuid:
                # if assay uuid is in current study return it, else do nothing, search next study
                if self.assay_uuid in study.assays.keys():
                    logger.info(f"Using provided Assay UUID: {self.assay_uuid}")
                    assay = study.assays[self.assay_uuid]
                    return assay, study
            else:
                # no assay specified, return first one or ask user
                assays_ = list(study.assays.keys())
                # only one assay or not interactive -> take first
                if len(assays_) == 1 or self.yes:
                    assay = study.assays[assays_[0]]
                    self.assay_uuid = assays_[0]
                    if self.yes and len(assays_) > 1:
                        multi_assay_study_warning(assays=assays_)
                    return assay, study
                # multiple assays and interactive, print uuids and ask for which
                self.assay_uuid = get_user_input_assay_study(assays_, study.assays)
                assay = study.assays[self.assay_uuid]
                return assay, study
        if self.assay_uuid is not None:
            msg = f"Assay with UUID {self.assay_uuid} not found in investigation."
            logger.error(msg)
            raise ParameterException(msg)
        return None, None

    def setup_sodar_params(self, args : argparse.Namespace, set_default : bool = False, with_dest : bool = False, dest_string :str= "project_uuid")->tuple[bool, argparse.Namespace] : # noqa: C901
        any_error = False

        # If SODAR info not provided, fetch from user's toml file
        toml_config = self.load_toml_config(getattr(args, "config", None))
        if toml_config:
            profile = getattr(args, "config_profile", "global")
            if profile not in toml_config:
                logger.error(f"Profile {profile} is not in toml_config, present vals are: {toml_config.keys()}")
                any_error = True
            args.sodar_server_url = args.sodar_server_url or toml_config.get(profile, {}).get("sodar_server_url")
            args.sodar_api_token = args.sodar_api_token or toml_config.get(profile, {}).get("sodar_api_token")

        # Check presence of SODAR URL and auth token.
        if not args.sodar_api_token:  # pragma: nocover
            logger.error(
                "SODAR API token not given on command line and not found in toml config files. Please specify using --sodar-api-token or set in config."
            )
            any_error = True
        if not args.sodar_server_url:  # pragma: nocover
            args.sodar_server_url="https://sodar.bihealth.org/"
            msg = "SODAR URL not given on command line and not found in toml config files. Please specify using --sodar-server-url, or set in config."
            if not set_default:
                logger.error(msg)
                any_error = True
            else:
                logger.warning(msg)
        if with_dest:
            dest = getattr(args, dest_string)
            is_dest_uuid = is_uuid(dest)
            if dest_string == "project_uuid" and not is_dest_uuid:
                logger.error("{} is not a valid UUID.", dest_string)
                any_error = True
            elif dest_string == "destination" and not is_dest_uuid:
                uuids = [p for p in dest.split("/") if is_uuid(p)]
                args.project_uuid = uuids[0]
                if len(uuids) != 1 or not dest.startswith("/"):
                    logger.error("{} is not a valid UUID or Path.", dest_string)
                    any_error = True
            #destiantion is UUID
            else:
                args.project_uuid = dest
        elif getattr(args, "project_uuid", None) is None:
            args.project_uuid = None #init project_uuid to none if not already in args for some snappy commands where project uuid is in config
        return any_error, args

    def load_toml_config(self, config):
    # Load configuration from TOML cubitkrc file, if any.
        if config:
            config_paths = [config,]
        else:
            config_paths = [GLOBAL_CONFIG_PATH, ]
        for config_path in config_paths:
            config_path = os.path.expanduser(os.path.expandvars(config_path))
            if os.path.exists(config_path):
                with open(config_path, "rt") as tomlf:
                    return toml.load(tomlf)
        logger.warning("Could not find any of the global configuration files {}.", config_paths)
        return None


