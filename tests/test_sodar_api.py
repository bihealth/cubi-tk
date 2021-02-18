"""Tests for ``cubi_tk.sodar.api``."""

import cattr
from cubi_tk.sodar.api import investigations, landing_zones, samplesheets

from . import factories


def test_investigations_get(requests_mock):
    args = {
        "sodar_url": "https://sodar.example.com/",
        "project_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    tpl = "%(sodar_url)ssamplesheets/api/investigation/retrieve/%(project_uuid)s"
    expected = factories.InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        tpl % args,
        headers={"Authorization": "Token %s" % args["sodar_api_token"]},
        json=cattr.unstructure(expected),
    )
    result = investigations.get(**args)
    _ = result
    # TODO: uncomment once sodar_uuid is written out for assay/study
    # assert expected == result


def test_samplesheets_get(requests_mock):
    args = {
        "sodar_url": "https://sodar.example.com/",
        "project_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    tpl = "%(sodar_url)ssamplesheets/api/export/json/%(project_uuid)s"
    expected = {"sample": "sheet"}
    requests_mock.register_uri(
        "GET",
        tpl % args,
        headers={"Authorization": "Token %s" % args["sodar_api_token"]},
        json=expected,
    )
    result = samplesheets.get(**args)
    assert expected == result


def test_samplesheets_upload(requests_mock, tmpdir):
    i_path = tmpdir / "i_example.txt"
    with i_path.open("wt") as i_file:
        print("TEST", file=i_file)

    args = {
        "sodar_url": "https://sodar.example.com/",
        "project_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
        "file_paths": [str(i_path)],
    }
    tpl = "%(sodar_url)ssamplesheets/api/import/%(project_uuid)s"
    expected = {"detail": "that worked"}
    requests_mock.register_uri(
        "POST",
        tpl % args,
        headers={"Authorization": "Token %s" % args["sodar_api_token"]},
        json=expected,
    )
    result = samplesheets.upload(**args)
    assert expected == result


def test_landingzones_get(requests_mock):
    args = {
        "sodar_url": "https://sodar.example.com/",
        "landing_zone_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    tpl = "%(sodar_url)slandingzones/api/retrieve/%(landing_zone_uuid)s"
    expected = factories.LandingZoneFactory()
    requests_mock.register_uri(
        "GET",
        tpl % args,
        headers={"Authorization": "Token %s" % args["sodar_api_token"]},
        json=cattr.unstructure(expected),
    )
    result = landing_zones.get(**args)
    assert expected == result


def test_landingzones_list(requests_mock):
    args = {
        "sodar_url": "https://sodar.example.com/",
        "project_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    tpl = "%(sodar_url)slandingzones/api/list/%(project_uuid)s"
    expected = [factories.LandingZoneFactory()]
    requests_mock.register_uri(
        "GET",
        tpl % args,
        headers={"Authorization": "Token %s" % args["sodar_api_token"]},
        json=cattr.unstructure(expected),
    )
    result = landing_zones.list(**args)
    assert expected == result


def test_landingzones_create(requests_mock):
    # Query for investigation
    i_args = {
        "sodar_url": "https://sodar.example.com/",
        "project_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    i_tpl = "%(sodar_url)ssamplesheets/api/investigation/retrieve/%(project_uuid)s"
    investigation = factories.InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        i_tpl % i_args,
        headers={"Authorization": "Token %s" % i_args["sodar_api_token"]},
        json=cattr.unstructure(investigation),
    )
    # Creation of landing zone.
    l_args = i_args.copy()
    l_tpl = "%(sodar_url)slandingzones/api/create/%(project_uuid)s"
    landing_zone = factories.LandingZoneFactory(project=i_args["project_uuid"])
    requests_mock.register_uri(
        "POST",
        l_tpl % i_args,
        headers={"Authorization": "Token %s" % i_args["sodar_api_token"]},
        json=cattr.unstructure(landing_zone),
    )
    result = landing_zones.create(**l_args)
    assert landing_zone == result


def test_landingzones_move(requests_mock):
    # Move landing zone
    m_args = {
        "sodar_url": "https://sodar.example.com/",
        "landing_zone_uuid": "46f4d0d7-b446-4a04-99c4-53cbffe952a3",
        "sodar_api_token": "token",
    }
    m_tpl = "%(sodar_url)slandingzones/api/submit/move/%(landing_zone_uuid)s"
    m_result = factories.LandingZoneFactory(sodar_uuid=m_args["landing_zone_uuid"])
    requests_mock.register_uri(
        "POST",
        m_tpl % m_args,
        headers={"Authorization": "Token %s" % m_args["sodar_api_token"]},
        json=cattr.unstructure(m_result),
    )
    # Retrieve landing zone.
    r_args = m_args.copy()
    r_tpl = "%(sodar_url)slandingzones/api/retrieve/%(landing_zone_uuid)s"
    r_result = m_result
    requests_mock.register_uri(
        "GET",
        r_tpl % r_args,
        headers={"Authorization": "Token %s" % r_args["sodar_api_token"]},
        json=cattr.unstructure(r_result),
    )
    result = landing_zones.move(**m_args)
    assert r_result == result
