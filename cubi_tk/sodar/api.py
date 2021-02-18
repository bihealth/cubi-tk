"""Client API code for SODAR."""

import contextlib
import pathlib
from types import SimpleNamespace
import typing

import cattr
from logzero import logger
import requests

from . import models


def _investigations_get(*, sodar_url, sodar_api_token, project_uuid):
    """Get investigation information."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]
    url_tpl = "%(sodar_url)s/samplesheets/api/investigation/retrieve/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}

    logger.debug("HTTP GET request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    # TODO: remove workaround once SODAR directly returns sodar_uuid
    tmp = r.json()
    for study_sodar_uuid, study in tmp["studies"].items():
        study["sodar_uuid"] = study_sodar_uuid
        for assay_sodar_uuid, assay in study["assays"].items():
            assay["sodar_uuid"] = assay_sodar_uuid
    return cattr.structure(tmp, models.Investigation)


#: Investigation-related API.
investigations = SimpleNamespace(get=_investigations_get)


def _samplesheets_get(*, sodar_url, sodar_api_token, project_uuid):
    """Get ISA-tab sample sheet from SODAR."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]
    url_tpl = "%(sodar_url)s/samplesheets/api/export/json/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}

    logger.debug("HTTP GET request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def _samplesheets_upload(*, sodar_url, sodar_api_token, project_uuid, file_paths):
    """Upload and replace ISA-tab sample sheet to SODAR."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]
    url_tpl = "%(sodar_url)s/samplesheets/api/import/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}

    logger.debug("HTTP POST request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    with contextlib.ExitStack() as stack:
        files = []
        for no, path in enumerate(file_paths):
            p = pathlib.Path(path)
            files.append(
                ("file_%d" % no, (p.name, stack.enter_context(p.open("rt")), "text/plain"))
            )
        r = requests.post(url, headers=headers, files=files)
    r.raise_for_status()
    return r.json()


#: Samplesheets-related API.
samplesheets = SimpleNamespace(get=_samplesheets_get, upload=_samplesheets_upload)


def _landingzones_get(*, sodar_url, sodar_api_token, landing_zone_uuid):
    """Return landing zones in project."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]
    url_tpl = "%(sodar_url)s/landingzones/api/retrieve/%(landing_zone_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "landing_zone_uuid": landing_zone_uuid}

    logger.debug("HTTP GET request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return cattr.structure(r.json(), models.LandingZone)


def _landingzones_list(*, sodar_url, sodar_api_token, project_uuid):
    """Return landing zones in project."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]
    url_tpl = "%(sodar_url)s/landingzones/api/list/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}

    logger.debug("HTTP GET request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return cattr.structure(r.json(), typing.List[models.LandingZone])


def _landingzones_create(*, sodar_url, sodar_api_token, project_uuid, assay_uuid=None):
    """Create landing zone in project."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]

    # Retrieve sample sheet for assay if not given.
    if not assay_uuid:
        investigation = investigations.get(
            sodar_url=sodar_url, sodar_api_token=sodar_api_token, project_uuid=project_uuid
        )
        if len(investigation.studies) != 1:  # pragma: no cover
            logger.error("Expected one study, found %d", len(investigation.studies))
            logger.info("Try specifying an explicit --assay parameter")
            raise Exception("Invalid number of studies in investigation!")  # TODO
        study = list(investigation.studies.values())[0]
        if len(study.assays) != 1:  # pragma: no cover
            logger.error("Expected one assay, found %d", len(study.assays))
            logger.info("Try specifying an explicit --assay parameter")
            raise Exception("Invalid number of assays in investigation!")  # TODO
        assay_uuid = list(study.assays.keys())[0]

    # Create landing zone through API.
    url_tpl = "%(sodar_url)s/landingzones/api/create/%(project_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "project_uuid": project_uuid}
    logger.debug("HTTP POST request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    data = {"assay": assay_uuid}
    r = requests.post(url, data=data, headers=headers)
    r.raise_for_status()
    return cattr.structure(r.json(), models.LandingZone)


def _landingzones_move(*, sodar_url, sodar_api_token, landing_zone_uuid):
    """Move landing zone with the given UUID."""
    while sodar_url.endswith("/"):
        sodar_url = sodar_url[:-1]

    # Move landing zone through API.
    url_tpl = "%(sodar_url)s/landingzones/api/submit/move/%(landing_zone_uuid)s"
    url = url_tpl % {"sodar_url": sodar_url, "landing_zone_uuid": landing_zone_uuid}
    logger.debug("HTTP POST request to %s", url)
    headers = {"Authorization": "Token %s" % sodar_api_token}
    r = requests.post(url, headers=headers)
    r.raise_for_status()
    return _landingzones_get(
        sodar_url=sodar_url,
        sodar_api_token=sodar_api_token,
        landing_zone_uuid=r.json()["sodar_uuid"],
    )


#: Landing zone-related API.
landing_zones = SimpleNamespace(
    list=_landingzones_list,
    create=_landingzones_create,
    move=_landingzones_move,
    get=_landingzones_get,
)
