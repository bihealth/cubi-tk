import argparse
from functools import reduce
from typing import List, Literal
import urllib.parse as urlparse
from uuid import UUID

import cattr
from loguru import logger
import requests

from cubi_tk.parsers import check_args_global_parser
from cubi_tk import api_models

from .exceptions import ParameterException, SodarApiException


#TODO: add studyname
def get_user_input_study(study_uuids):
    """Display available study UUIDS and let User choose which one to use.

    :param assays: Assays UUIDs as found in Studies.
    :type assays: list
    """
    logger.warning("Multiple studies present, which one do you want to choose?")
    i = 0
    while i < len(study_uuids):
        logger.warning("{}: {}", i+1, study_uuids[i] )
        i+=1
    study_num = 0
    while (study_num<= 0 or study_num> len(study_uuids)):
        study_num = input("Please enter the index of the Study UUID (e.g 2):")
        study_num =int(study_num)
    return study_uuids[study_num-1]

#TODO: add assayname
def get_user_input_assay_uuid(assay_uuids):
    """Display available assay UUIDS and let User choose which one to use.

    :param assays: Assays UUIDs as found in Studies.
    :type assays: list
    """
    logger.warning("No --assay-uuid specified but multiple assays present, which assay do you want to choose?")
    i = 0
    while i < len(assay_uuids):
        logger.warning("{}: {}", i+1, assay_uuids[i] )
        i+=1
    assay_num = 0
    while (assay_num<= 0 or assay_num> len(assay_uuids)):
        assay_num = input("Please enter the index of the Assay UUID (e.g 2):")
        assay_num =int(assay_num)
    return assay_uuids[assay_num-1]


def multi_assay_study_warning(content, string = "Assays"):
    """Display warning for multi-assay study.

    :param assays: Assays UUIDs as found in Studies.
    :type assays: list
    """
    multi_assay_str = "\n".join(content)
    logger.warning(
        f"Project contains multiple {string}, will only consider UUID '{content[0]}'.\n"
        f"All available UUIDs:\n{multi_assay_str}"
    )

SODAR_API_VERSION=1.0

LANDING_ZONE_STATES = ["ACTIVE", "FAILED", "VALIDATING"]

class SodarApi:
    def __init__(self, args: argparse.Namespace, set_default = False, with_dest = False, dest_string = "project_uuid"):
       any_error, args= check_args_global_parser(args, set_default = set_default, with_dest=with_dest, dest_string= dest_string)
       if any_error:
            raise ParameterException('Sodar args missing')
       self.sodar_server_url = args.sodar_server_url
       self.sodar_api_token = args.sodar_api_token #TODO: remove and just use for header
       self.project_uuid = args.project_uuid
       self.assay_uuid = getattr(args, "assay_uuid", None)
       self.lz_path = getattr(args, "destination", None) #if destiantion exists and destination is lz path (!= project_uuid), set lz_path
       if self.lz_path == self.project_uuid:
           self.lz_path = None
       self.yes = getattr(args, "yes", False)
       self.sodar_headers = {
           "samplesheets": {
               "Authorization": "token {}".format(self.sodar_api_token),
                'Accept': f'application/vnd.bihealth.sodar.samplesheets+json; version={SODAR_API_VERSION}'},
            "landingzones": {
               "Authorization": "token {}".format(self.sodar_api_token),
                'Accept': f'application/vnd.bihealth.sodar.landingzones+json; version={SODAR_API_VERSION}'},
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
        if params:
            url += "?" + urlparse.urlencode(params)

        if method == "get":
            logger.debug(f"HTTP GET request to {url} with headers {self.sodar_headers[api]}")
            response = requests.get(url, headers=self.sodar_headers[api])
        elif method == "post":
            logger.debug(f"HTTP POST request to {url} with headers {self.sodar_headers[api]}, files {files}, and data {data}")
            response = requests.post(url, headers=self.sodar_headers[api], files=files, data=data)
        else:
            raise ValueError("Unknown HTTP method.")

        if response.status_code != 200 and response.status_code != 201:
            raise SodarApiException(f"API response: {response.text} and status code: {response.status_code}")

        return response.json()

    # Samplesheet api calls
    def get_samplesheet_export(self, get_all = False) -> dict[str, dict]:
        logger.debug("Exporting samplesheet..")
        samplesheet = self._api_call("samplesheets", "export/json")
        if get_all:
            logger.debug("Returning all samplesheets")
            return samplesheet

        study_name = list(samplesheet["studies"].keys())[0]
        assay_name = list(samplesheet["assays"].keys())[0]
        if len(samplesheet["studies"]) > 1 or len(samplesheet["assays"]) > 1:
            assay_name, study_name = self.get_assay_from_uuid()
            assay_name = assay_name.file_name
            study_name= study_name.file_name

        return {
            "investigation": {
                "path": samplesheet["investigation"]["path"],
                "tsv": samplesheet["investigation"]["tsv"],
            },
            "studies": {study_name : samplesheet["studies"][study_name]},
            "assays": {assay_name : samplesheet["assays"][assay_name]},
        }

    def get_samplesheet_retrieve(self) -> api_models.Investigation:
        logger.debug("Get investigation information.")
        investigationJson = self._api_call("samplesheets", "investigation/retrieve")
        investigation = cattr.structure(investigationJson, api_models.Investigation)
        logger.debug(f"Got investigation: {investigation}")
        return investigation


    def post_samplesheet_import(
        self,
        files_dict: dict[str, tuple[str, str]],
    ) -> int:
        logger.debug("Posting samplesheet..")
        for key, value in files_dict:
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

    # landingzone Api calls
    def get_landingzone_retrieve(self, lz_uuid: UUID = None) -> api_models.LandingZone | None:
        logger.debug("Retrieving Landing Zone ...")
        try:
            landingzone = self._api_call("landingzones", "retrieve", dest_uuid=lz_uuid) #if None: assume projectuuid is lz_uuid
            landingzone = cattr.structure(landingzone, api_models.LandingZone)
            self.project_uuid = landingzone.project
            return landingzone
        except SodarApiException as e:
            logger.error(f"Failed to retrieve Landingzone:\n{e}")
            return None

    def get_landingzone_list(self, sort_reverse = False, filter_for_state=LANDING_ZONE_STATES) -> List[api_models.LandingZone]:
        logger.debug("Creating new Landing Zone...")
        landingzones_json = self._api_call("landingzones", "list")
        landingzones = cattr.structure(landingzones_json, List[api_models.LandingZone])
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

    def post_landingzone_create(self)-> api_models.LandingZone | None:
        logger.debug("Creating new Landing Zone...")
        if not self.assay_uuid:
            self.get_assay_from_uuid()
        try:
            ret_val = self._api_call("landingzones", "create", method="post",
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
            logger.error(f"Failed to create Landingzone:\n{e}")
            return None


    def post_landingzone_submit_move(self, lz_uuid)-> UUID | None:
        logger.debug("Moving landing zone with the given UUID")
        try:
            ret_val = self._api_call("landingzones", "submit/move", method="post", dest_uuid=lz_uuid)
            new_uuid = ret_val["sodar_uuid"]
            logger.info("Landingzone with UUID {} moved successfully.", new_uuid)
            return new_uuid
        except SodarApiException as e:
            logger.error(f"Failed to move Landingzone:\n{e}")
            return None

    def post_landingzone_submit_validate(self, lz_uuid)-> UUID | None:
        logger.debug("Validating landing zone with the given UUID")
        try:
            ret_val = self._api_call("landingzones", "submit/validate", method="post", dest_uuid=lz_uuid)
            new_uuid = ret_val["sodar_uuid"]
            logger.info("Landingzone with UUID {} valiated successfully.", new_uuid)
            return new_uuid
        except SodarApiException as e:
            logger.error(f"Failed to validate Landingzone:\n{e}")
            return None

    # helper functions
    def get_assay_from_uuid(self):
        investigation = self.get_samplesheet_retrieve()
        studies = investigation.studies.values()
        #if assay_uuid given and multiple studies iterate through all studies and find assay
        #if mulitple staudies and yes, iterate through first study
        #if multiple studies, no asssay uuid and not yes, let user decide which study to use
        if(len(studies) > 1 and not self.assay_uuid):
            study_keys =investigation.studies.keys()
            if not self.yes:
                study = get_user_input_study(study_keys)
                studies = [study]
            else:
                multi_assay_study_warning(study_keys, string="studies")

        for study in studies:
            if self.assay_uuid:
                #bug fix for rare case that multiple studies and multiple assays exist
                if self.assay_uuid in study.assays.keys():
                    logger.info(f"Using provided Assay UUID: {self.assay_uuid}")
                    assay = study.assays[self.assay_uuid]
                    return assay, study
            assays_ = list(study.assays.keys())
            #only one assay or not interactive -> take first
            if len(assays_) == 1 or self.yes:
                assay = study.assays[assays_[0]]
                self.assay_uuid = assays_[0]
                if self.yes and len(assays_) > 1:
                    multi_assay_study_warning(assays=assays_)
                return assay, study
            #multiple assays and interactive, print uuids and ask for which
            self.assay_uuid = get_user_input_assay_uuid(assay_uuids=assays_)
            assay = study.assays[self.assay_uuid]
            return assay, study
        if self.assay_uuid is not None:
            msg = f"Assay with UUID {self.assay_uuid} not found in investigation."
            logger.error(msg)
            raise ParameterException(msg)
        return None


