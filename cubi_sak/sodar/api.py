"""Client API code for SODAR."""

from types import SimpleNamespace

from logzero import logger
import requests

from cubi_sak.exceptions import ParameterException


def _samplesheets_get(*, sodar_url, sodar_auth_token, project_uuid, as_isa_tab=True):
    """Get ISA-tab sample sheet from SODAR."""
    url_tpl = "%(sodar_url)s/samplesheets/api/remote/get/%(project_uuid)s/%(api_key)s%(isa)s"
    url = url_tpl % {
        "sodar_url": sodar_url,
        "project_uuid": project_uuid,
        "api_key": sodar_auth_token,
        "isa": "?isa=1" if as_isa_tab else "",
    }

    logger.debug("HTTP GET request to %s", url)
    r = requests.get(url)
    r.raise_for_status()
    return r.json()


#: Samplesheets-related API.
samplesheets = SimpleNamespace(get=_samplesheets_get)


class _SheetClient:
    """Provide commands for sample sheets."""

    def __init__(self, owner):
        #: Owning ``Client`` instance.
        self.owner = owner

    def get(self, project_uuid=None):
        """Get sample sheet, ``project_uuid`` can override default ``project_uuid`` from ``self.owner``."""
        if not project_uuid and not self.owner.project_uuid:
            raise ParameterException("Both Client and method's project_uuid argument missing.")
        return _samplesheets_get(
            sodar_url=self.owner.sodar_url,
            sodar_auth_token=self.owner.sodar_auth_token,
            project_uuid=project_uuid or self.owner.project_uuid,
        )


class Client:
    """The API client."""

    def __init__(self, sodar_url, sodar_auth_token, project_uuid=None):
        #: URL to SODAR.
        self.sodar_url = sodar_url
        #: SODAR auth token.
        self.sodar_auth_token = sodar_auth_token
        #: Project UUID to use by default.
        self.project_uuid = project_uuid
        #: Client for accessing sample sheets.
        self.samplesheets = _SheetClient(self)
