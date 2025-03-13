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
def get_assay_from_uuid(sodar_server_url, sodar_api_token, project_uuid, assay_uuid):
    investigation = api.samplesheet.retrieve(
            sodar_url=sodar_server_url,
            sodar_api_token=sodar_api_token,
            project_uuid=project_uuid,
        )
    assay = None
    for study in investigation.studies.values():
        for remote_assay_uuid in study.assays.keys():
            if assay_uuid== remote_assay_uuid:
                assay = study.assays[remote_assay_uuid]
                break
    if assay is None:
        msg = f"Assay with UUID {assay_uuid} not found in investigation."
        logger.error(msg)
        raise ParameterException(msg)
    return assay


class SodarAPI:
    def __init__(self, args: argparse.Namespace):
       any_error, args= check_args_global_parser(args, with_dest=True)
       if any_error:
            raise ParameterException('Sodar args missing')
       self.sodar_server_url = args.sodar_server_url
       self.sodar_api_token = args.sodar_api_token
       self.project_uuid = args.project_uuid



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
        if len(samplesheet["studies"]) > 1:
            raise NotImplementedError("Only single-study projects are supported.")
        study = list(samplesheet["studies"].keys())[0]
        if len(samplesheet["assays"]) > 1:
            raise NotImplementedError("Only single-assay projects are supported.")
        assay = list(samplesheet["assays"].keys())[0]

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
