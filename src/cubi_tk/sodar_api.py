import argparse
from functools import reduce
from typing import Literal
import urllib.parse as urlparse

from loguru import logger
import requests

from cubi_tk.parsers import check_args_global_parser

from .exceptions import ParameterException, SodarApiException

from sodar_cli import api

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

class SodarApi:
    def __init__(self, args: argparse.Namespace, set_default = False, with_dest = False, dest_string = "project_uuid"):
       any_error, args= check_args_global_parser(args, set_default = set_default, with_dest=with_dest, dest_string= dest_string)
       if any_error:
            raise ParameterException('Sodar args missing')
       self.sodar_server_url = args.sodar_server_url
       self.sodar_api_token = args.sodar_api_token #TODO: remove and just use for header
       self.project_uuid = getattr(args, "project_uuid", None)
       self.assay_uuid = getattr(args, "assay_uuid", None)
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
    ) -> dict:
        # need to add trailing slashes to all parts of the URL for urljoin to work correctly
        # afterward remove the final trailing slash from the joined URL
        base_url_parts = [
            part if part.endswith("/") else f"{part}/"
            for part in (self.sodar_server_url, api, "api", action, self.project_uuid)
        ]
        url = reduce(urlparse.urljoin, base_url_parts)[:-1]
        if params:
            url += "?" + urlparse.urlencode(params)

        if method == "get":
            response = requests.get(url, headers=self.sodar_headers[api])
        elif method == "post":
            response = requests.post(url, headers=self.sodar_headers[api], files=files, data=data)
        else:
            raise ValueError("Unknown HTTP method.")

        if response.status_code != 200:
            raise SodarApiException(f"API response: {response.text}")

        return response.json()

    # Samplesheet api calls
    def get_samplesheet_export(self, get_all = False) -> dict[str, dict[str, str]]:
        samplesheet = self._api_call("samplesheets", "export/json")
        if get_all:
            return samplesheet
        # Consider: support multi-assay and multi-study projects?
        # -> would require proper ISA parsing to handle assay<>study relations
        # if len(samplesheet["studies"]) > 1:
            #raise NotImplementedError("Only single-study projects are supported.")
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

    def post_samplesheet_import(
        self,
        files_dict: dict[str, tuple[str, str]],
    ):
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
    # helper functions
    def get_assay_from_uuid(self):
        investigation = api.samplesheet.retrieve(
                sodar_url=self.sodar_server_url,
                sodar_api_token=self.sodar_api_token,
                project_uuid=self.project_uuid,
            )
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


