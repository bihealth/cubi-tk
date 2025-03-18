import argparse
from functools import reduce
from typing import Literal
import urllib.parse as urlparse

from loguru import logger
import requests

from cubi_tk.parsers import check_args_global_parser

from .exceptions import ParameterException, SodarAPIException

from sodar_cli import api

#TODO: integrate into new SodarApi class
def get_assay_from_uuid(sodar_server_url, sodar_api_token, project_uuid, assay_uuid = None, yes = False):
    investigation = api.samplesheet.retrieve(
            sodar_url=sodar_server_url,
            sodar_api_token=sodar_api_token,
            project_uuid=project_uuid,
        )
    studies = investigation.studies.values()
    #if assay_uuid given and multiple studies iterate through all studies and find assay
    #if mulitple staudies and yes, iterate through first study
    #if multiple studies, no asssay uuid and not yes, let user decide which study to use
    if(len(studies) > 1 and not assay_uuid):
        study_keys =investigation.studies.keys()
        if not yes:
            study = get_user_input_study(study_keys)
            studies = [study]
        else:
            multi_assay_study_warning(study_keys, string="studies")

    for study in studies:
        if assay_uuid:
            #bug fix for rare case that multiple studies and multiple assays exist
            if assay_uuid in study.assays.keys():
                logger.info(f"Using provided Assay UUID: {assay_uuid}")
                assay = study.assays[assay_uuid]
                return assay, study
        #will only iterate through first study, if multiple studys present
        assays_ = list(study.assays.keys())
        #only one assay
        if len(assays_) == 1:
            return study.assays[assays_[0]], study
        # multiple assays, if not interactive take fisrt
        if yes:
            multi_assay_study_warning(assays=assays_)
            for _assay_uuid in assays_:
                assay = study.assays[_assay_uuid]
                return assay, study
        #interactive, print uuids and ask for which
        assay_uuid = get_user_input_assay_uuid(assay_uuids=assays_)
        assay = study.assays[assay_uuid]
        return assay, study
    if assay_uuid is not None:
        msg = f"Assay with UUID {assay_uuid} not found in investigation."
        logger.error(msg)
        raise ParameterException(msg)
    return None


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


class SodarAPI:
    def __init__(self, args: argparse.Namespace):
       any_error, args= check_args_global_parser(args, with_dest=True)
       if any_error:
            raise ParameterException('Sodar args missing')
       self.sodar_server_url = args.sodar_server_url
       self.sodar_api_token = args.sodar_api_token
       self.project_uuid = args.project_uuid
       self.assay_uuid = getattr(args, "assay_uuid", None)
       self.yes = getattr(args, "yes", False)


    def _base_api_header(self) -> dict[str, str]:
        # FIXME: only add versioning header once SODAR API v1.0 is released
        # (this will introduce individual versioning for specific calls and break the general versioning)
        sodar_headers = {
            "Authorization": "token {}".format(self.sodar_api_token),
            # 'Accept': f'application/vnd.bihealth.sodar+json; version={SODAR_API_VERSION}',
        }
        return sodar_headers

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
            response = requests.get(url, headers=self._base_api_header())
        elif method == "post":
            response = requests.post(url, headers=self._base_api_header(), files=files, data=data)
        else:
            raise ValueError("Unknown HTTP method.")

        if response.status_code != 200:
            raise SodarAPIException(f"API response: {response.text}")

        return response.json()

    def get_ISA_samplesheet(self) -> dict[str, dict[str, str]]:
        samplesheet = self._api_call("samplesheets", "export/json")

        # Consider: support multi-assay and multi-study projects?
        # -> would require proper ISA parsing to handle assay<>study relations
        # if len(samplesheet["studies"]) > 1:
            #raise NotImplementedError("Only single-study projects are supported.")
        study = list(samplesheet["studies"].keys())[0]
        assay = list(samplesheet["assays"].keys())[0]
        if len(samplesheet["studies"]) > 1 or len(samplesheet["assays"]) > 1:
            assay, study = get_assay_from_uuid(
                self.sodar_server_url,
                self.sodar_api_token,
                self.project_uuid,
                self.assay_uuid,
                self.yes,
                )
            assay = assay.file_name
            study= study.file_name

        return {
            "investigation": {
                "filename": samplesheet["investigation"]["path"],
                "content": samplesheet["investigation"]["tsv"],
            },
            "study": {"filename": study, "content": samplesheet["studies"][study]["tsv"]},
            "assay": {"filename": assay, "content": samplesheet["assays"][assay]["tsv"]},
        }

    def upload_ISA_samplesheet(
        self,
        investigation: tuple[str, str],
        study: tuple[str, str],
        assay: tuple[str, str],
    ):
        try:
            ret_val = self._api_call(
                "samplesheets",
                "import",
                method="post",
                files={
                    "file_investigation": (*investigation, "text/plain"),
                    "file_study": (*study, "text/plain"),
                    "file_assay": (*assay, "text/plain"),
                },
            )
            if "sodar_warnings" in ret_val:
                logger.info("ISA-tab uploaded with warnings.")
                for warning in ret_val["sodar_warnings"]:
                    logger.warning(f"SODAR warning: {warning}")
            else:
                logger.info("ISA-tab uploaded successfully.")
            return 0
        except SodarAPIException as e:
            logger.error(f"Failed to upload ISA-tab:\n{e}")
            return 1
