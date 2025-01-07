import argparse
import os
from typing import Literal

from logzero import logger
import requests

from .exceptions import SodarAPIException


# FIXME: maybe this should be used as a MixIn for other functions?
class SodarAPI:
    def __init__(self, sodar_url: str, sodar_api_token: str, project_uuid: str):
        self.sodar_url = sodar_url
        self.sodar_api_token = sodar_api_token
        self.project_uuid = project_uuid

    @staticmethod
    def setup_argparse(parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        group_sodar = parser.add_argument_group("SODAR-related")

        # load default from toml file
        # consider: mark token as sensitive
        group_sodar.add_argument(
            "--sodar-url",
            default=os.environ.get("SODAR_URL", "https://sodar.bihealth.org/"),
            help="URL to SODAR, defaults to SODAR_URL environment variable or fallback to https://sodar.bihealth.org/",
        )
        group_sodar.add_argument(
            "--sodar-api-token",
            default=os.environ.get("SODAR_API_TOKEN", None),
            help="Authentication token when talking to SODAR.  Defaults to SODAR_API_TOKEN environment variable.",
        )
        group_sodar.add_argument(
            "project_uuid",
            help="SODAR project UUID",
        )

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
        url = self.sodar_url + api + "/api/" + action + "/" + self.project_uuid
        if params:
            url += "?" + "&".join([f"{k}={v}" for k, v in params.items()])

        if method == "get":
            response = requests.get(url, headers=self._base_api_header())
        elif method == "post":
            response = requests.post(url, headers=self._base_api_header(), files=files, data=data)

        if response.status_code != 200:
            raise SodarAPIException(f"Negative API response: {response.text}")

        return response.json()

    def get_ISA_samplesheet(self) -> dict[str, tuple[str, str]]:
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
            "investigation": (
                samplesheet["investigation"]["path"],
                samplesheet["investigation"]["tsv"],
            ),
            "study": (study, samplesheet["studies"][study]["tsv"]),
            "assay": (assay, samplesheet["assays"][assay]["tsv"]),
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
                files=[
                    ("file", investigation),
                    ("file", study),
                    ("file", assay),
                ],
            )
            if "sodar_warnings" in ret_val:
                for warning in ret_val["sodar_warnings"]:
                    logger.warning(f"SODAR warning: {warning}")
            return 0
        except SodarAPIException as e:
            logger.error(f"Failed to upload ISA-tab:\n{e}")
            return 1
