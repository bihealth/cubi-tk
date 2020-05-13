"""Client API code for SODAR."""

import io
from types import SimpleNamespace
import typing

from altamisa.isatab import (
    InvestigationInfo,
    Study,
    Assay,
    InvestigationReader,
    StudyReader,
    AssayReader,
)
import attr
from logzero import logger
import requests

from ..exceptions import ParameterException, UnsupportedIsaTabFeatureException


def _samplesheets_get(*, sodar_url, sodar_api_token, project_uuid):
    """Get ISA-tab sample sheet from SODAR."""
    url_tpl = "%(sodar_url)s/samplesheets/api/export/json/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}

    logger.debug("HTTP GET request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


#: Samplesheets-related API.
samplesheets = SimpleNamespace(get=_samplesheets_get)


@attr.s(frozen=True, auto_attribs=True)
class IsaData:
    """Bundle together investigation, studies, assays from one project."""

    #: Investigation.
    investigation: InvestigationInfo
    #: Investigation file name.
    investigation_filename: str
    #: Tuple of studies.
    studies: typing.Dict[str, Study]
    #: Tuple of assays.
    assays: typing.Dict[str, Assay]


class _SheetClient:
    """Provide commands for sample sheets."""

    def __init__(self, owner):
        #: Owning ``Client`` instance.
        self.owner = owner

    def get_raw(self, project_uuid=None):
        """Get raw sample sheet data, ``project_uuid`` can override default ``project_uuid`` from ``self.owner``."""
        if not project_uuid and not self.owner.project_uuid:
            raise ParameterException("Both Client and method's project_uuid argument missing.")
        return _samplesheets_get(
            sodar_url=self.owner.sodar_url,
            sodar_api_token=self.owner.sodar_api_token,
            project_uuid=project_uuid or self.owner.project_uuid,
        )

    def get(self, project_uuid=None) -> IsaData:
        raw_data = self.get_raw(project_uuid)
        investigation = InvestigationReader.from_stream(
            input_file=io.StringIO(raw_data["investigation"]["tsv"]),
            filename=raw_data["investigation"]["path"],
        ).read()
        studies = {
            path: StudyReader.from_stream(
                study_id=path, input_file=io.StringIO(details["tsv"]), filename=path
            ).read()
            for path, details in raw_data["studies"].items()
        }
        if len(studies) > 1:  # pragma: nocover
            raise UnsupportedIsaTabFeatureException("More than one study found!")
        study = list(studies.values())[0]
        assays = {
            path: AssayReader.from_stream(
                study_id=study.file,
                assay_id=path,
                input_file=io.StringIO(details["tsv"]),
                filename=path,
            ).read()
            for path, details in raw_data["assays"].items()
        }
        return IsaData(investigation, raw_data["investigation"]["path"], studies, assays)


class Client:
    """The API client."""

    def __init__(self, sodar_url, sodar_api_token, project_uuid=None):
        #: URL to SODAR.
        self.sodar_url = sodar_url
        #: SODAR auth token.
        self.sodar_api_token = sodar_api_token
        #: Project UUID to use by default.
        self.project_uuid = project_uuid
        #: Client for accessing sample sheets.
        self.samplesheets = _SheetClient(self)
