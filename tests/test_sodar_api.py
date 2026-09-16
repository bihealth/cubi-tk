from argparse import Namespace
import os

import cattr

import pytest
from unittest.mock import patch, MagicMock

from cubi_tk.api_models import IrodsDataObject
from cubi_tk.sodar_api import GLOBAL_CONFIG_PATH, SodarApi
from cubi_tk.exceptions import SodarApiException
from tests.factories import InvestigationFactory, LandingZoneFactory


@pytest.fixture
def sodar_api_args():
    return {
        "config": None,
        "sodar_server_url": "https://sodar-staging.bihealth.org/",
        "sodar_api_token": "token123",
        "project_uuid": "123e4567-e89b-12d3-a456-426655440000",
    }


@pytest.fixture
def sodar_api_instance(sodar_api_args):
    return SodarApi(Namespace(**sodar_api_args))


def test_sodar_api_check_args(sodar_api_args, mock_toml_config, fs):
    # Check args is automatically called in __init__, so we only need to create instances for testing
    args = sodar_api_args.copy()

    # Successful baseline creation
    SodarApi(Namespace(**args))

    # No toml config available, fail if any value is not given, or malformed
    args["sodar_server_url"] = ""
    with pytest.raises(SystemExit):
        SodarApi(Namespace(**args))
    args["sodar_server_url"] = "https://sodar-staging.bihealth.org/"
    args["sodar_api_token"] = ""
    with pytest.raises(SystemExit):
        SodarApi(Namespace(**args))
    args["sodar_api_token"] = "token"
    args["project_uuid"] = "not a uuid"
    with pytest.raises(SystemExit):
        SodarApi(Namespace(**args), with_dest=True)

    # With toml config available, only project_uuid is required
    fs.create_file(os.path.expanduser(GLOBAL_CONFIG_PATH), contents=mock_toml_config)
    SodarApi(
        Namespace(
            config=None,
            sodar_server_url="",
            sodar_api_token="",
            project_uuid="123e4567-e89b-12d3-a456-426655440000",
        ),
        with_dest=True,
    )


@patch("cubi_tk.sodar_api.requests.get")
@patch("cubi_tk.sodar_api.requests.post")
def test_sodar_api_api_call(mock_post, mock_get, sodar_api_instance):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(return_value={"test": "test"})

    # Test simple request
    out = sodar_api_instance._api_call("samplesheets", "test")
    mock_get.assert_called_with(
        "https://sodar-staging.bihealth.org/samplesheets/api/test/123e4567-e89b-12d3-a456-426655440000",
        headers={
            "Authorization": "token token123",
            "Accept": "application/vnd.bihealth.sodar.samplesheets+json; version=1.1",
        },
    )
    assert out == {"test": "test"}

    # Test request with params
    # FIXME: also test proper URL encoding of params?
    out = sodar_api_instance._api_call("samplesheets", "test", params={"test": "test"})
    mock_get.assert_called_with(
        "https://sodar-staging.bihealth.org/samplesheets/api/test/123e4567-e89b-12d3-a456-426655440000?test=test",
        headers={
            "Authorization": "token token123",
            "Accept": "application/vnd.bihealth.sodar.samplesheets+json; version=1.1",
        },
    )

    # Test request with error
    mock_get.return_value.status_code = 123
    with pytest.raises(SodarApiException):
        out = sodar_api_instance._api_call("samplesheets", "test/action")

    # Test post request with extra data
    mock_post.return_value.status_code = 200
    out = sodar_api_instance._api_call(
        "landingzones", "fake/upload", method="post", data={"test": "test2"}
    )
    mock_post.assert_called_once_with(
        "https://sodar-staging.bihealth.org/landingzones/api/fake/upload/123e4567-e89b-12d3-a456-426655440000",
        headers={
            "Authorization": "token token123",
            "Accept": "application/vnd.bihealth.sodar.landingzones+json; version=1.0",
        },
        files=None,
        data={"test": "test2"},
    )


def test_sodar_api_get_samplesheet_export(requests_mock, sodar_api_instance):
    ret_json = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}},
        "date_modified": "2021-09-01T12:00:00Z",
    }
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/export/json/123e4567-e89b-12d3-a456-426655440000",
        json=ret_json,
        status_code=200,
    )
    expected = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}},
    }
    assert expected == sodar_api_instance.get_samplesheet_export()
    ret_json = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}, "s_Study_1.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}, "a_name_1": {"tsv": ""}},
        "date_modified": "2021-09-01T12:00:00Z",
    }
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/export/json/123e4567-e89b-12d3-a456-426655440000",
        json=ret_json,
        status_code=200,
    )
    expected = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}},
    }
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(InvestigationFactory()),
        status_code=200,
    )
    assert expected == sodar_api_instance.get_samplesheet_export()


def test_sodar_api_get_samplesheet_file_list(requests_mock, sodar_api_instance):
    ret_json = [
        {
            "name": "File name",
            "type": "file",
            "path": "collection/File Name",
            "size": "10",
            "modify_time": "2025-01-01 00:00:00",
            "checksum": "1234567890",
        },
        {
            "name": "collection",
            "type": "obj",
            "path": "collection",
            "size": "1",
            "modify_time": "2025-01-01 00:00:00",
            "checksum": "000000",
        },
    ]
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/file/list/123e4567-e89b-12d3-a456-426655440000",
        json=ret_json,
        status_code=200,
    )

    expected = [
        IrodsDataObject(
            name="File name",
            type="file",
            path="collection/File Name",
            size=10,
            modify_time="2025-01-01 00:00:00",
            checksum="1234567890",
        ),
        IrodsDataObject(
            name="collection",
            type="obj",
            path="collection",
            size=1,
            modify_time="2025-01-01 00:00:00",
            checksum="000000",
        ),
    ]

    assert expected == sodar_api_instance.get_samplesheet_file_list()


def test_sodar_api_get_landingzone_list(requests_mock, sodar_api_instance):
    lz1 = LandingZoneFactory(
        status="ACTIVE",
        date_modified="2025-01-02T00:00:00Z",
        assay="assay-1",
        irods_path="/testZone/path/to/zone/1",
    )
    lz2 = LandingZoneFactory(
        status="FAILED",
        date_modified="2025-01-01T00:00:00Z",
        assay="assay-1",
        irods_path="/testZone/path/to/zone/2",
    )
    lz3 = LandingZoneFactory(
        status="VALIDATING",
        date_modified="2025-01-03T00:00:00Z",
        assay="assay-2",
        irods_path="/testZone/path/to/zone/1",
    )
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/landingzones/api/list/123e4567-e89b-12d3-a456-426655440000",
        json=[
            cattr.unstructure(lz1),
            cattr.unstructure(lz2),
            cattr.unstructure(lz3),
        ],
        status_code=200,
    )

    assert [lz2, lz1, lz3] == sodar_api_instance.get_landingzone_list()

    sodar_api_instance.assay_uuid = "assay-1"
    sodar_api_instance.lz_path = "/testZone/path/to/zone/1"
    assert [lz1] == sodar_api_instance.get_landingzone_list(
        sort_by="creation", sort_reverse=True, filter_for_state=["ACTIVE"]
    )


def test_sodar_api_get_landingzone_list_errors(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/landingzones/api/list/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )

    assert sodar_api_instance.get_landingzone_list() is None


def test_sodar_api_get_samplesheet_investigation_retrieve(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    assert investigation == sodar_api_instance.get_samplesheet_investigation_retrieve()


def test_sodar_api_get_samplesheet_investigation_retrieve_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_samplesheet_investigation_retrieve() is None


def test_sodar_api_get_samplesheet_remote(requests_mock, sodar_api_instance):
    ret_json = {"investigation": {"path": "i_Investigation.txt", "tsv": ""}}
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/remote/get/123e4567-e89b-12d3-a456-426655440000?isa=1",
        json=ret_json,
        status_code=200,
    )
    assert ret_json == sodar_api_instance.get_samplesheet_remote()


def test_sodar_api_get_samplesheet_remote_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/remote/get/123e4567-e89b-12d3-a456-426655440000?isa=1",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_samplesheet_remote() is None


def test_sodar_api_get_samplesheet_file_list_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/file/list/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_samplesheet_file_list() is None


def test_sodar_api_post_samplesheet_import(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/import/123e4567-e89b-12d3-a456-426655440000",
        json={},
        status_code=200,
    )
    ret = sodar_api_instance.post_samplesheet_import({"file1": ("file1.txt", "content")})
    assert ret == 0


def test_sodar_api_post_samplesheet_import_with_warnings(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/import/123e4567-e89b-12d3-a456-426655440000",
        json={"sodar_warnings": ["warning1"]},
        status_code=200,
    )
    ret = sodar_api_instance.post_samplesheet_import({"file1": ("file1.txt", "content")})
    assert ret == 0


def test_sodar_api_post_samplesheet_import_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/import/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    ret = sodar_api_instance.post_samplesheet_import({"file1": ("file1.txt", "content")})
    assert ret == 1


def test_sodar_api_post_samplesheet_deletion_request_create(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/request/create/123e4567-e89b-12d3-a456-426655440000",
        json={},
        status_code=200,
    )
    ret = sodar_api_instance.post_samplesheet_deletion_request_create(
        "/some/path", description="desc"
    )
    assert ret == 0


def test_sodar_api_post_samplesheet_deletion_request_create_error(
    requests_mock, sodar_api_instance
):
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/request/create/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    ret = sodar_api_instance.post_samplesheet_deletion_request_create("/some/path")
    assert ret == 1


def test_sodar_api_get_pending_deletion_requests(requests_mock, sodar_api_instance):
    ret_json = [
        {"path": "/some/path/sample1"},
        {"path": "/some/path/sample2"},
    ]
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/requests/123e4567-e89b-12d3-a456-426655440000",
        json=ret_json,
        status_code=200,
    )
    assert ret_json == sodar_api_instance.get_pending_deletion_requests()
    assert [ret_json[0]] == sodar_api_instance.get_pending_deletion_requests(["sample1"])


def test_sodar_api_get_pending_deletion_requests_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/requests/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_pending_deletion_requests() is None


def test_sodar_api_accept_deletion_request(requests_mock, sodar_api_instance):
    from cubi_tk.api_models import IrodsDataRequest

    request_obj = IrodsDataRequest(
        sodar_uuid="123e4567-e89b-12d3-a456-426655440001",
        action="delete",
        status="ACTIVE",
        path="/some/path",
        project="123e4567-e89b-12d3-a456-426655440000",
        date_created="2025-01-01T00:00:00Z",
        user="123e4567-e89b-12d3-a456-426655440002",
    )
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/request/accept/123e4567-e89b-12d3-a456-426655440001",
        json={},
        status_code=200,
    )
    assert sodar_api_instance.accept_deletion_request(request_obj) == 0


def test_sodar_api_accept_deletion_request_error(requests_mock, sodar_api_instance):
    from cubi_tk.api_models import IrodsDataRequest

    request_obj = IrodsDataRequest(
        sodar_uuid="123e4567-e89b-12d3-a456-426655440001",
        action="delete",
        status="ACTIVE",
        path="/some/path",
        project="123e4567-e89b-12d3-a456-426655440000",
        date_created="2025-01-01T00:00:00Z",
        user="123e4567-e89b-12d3-a456-426655440002",
    )
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/samplesheets/api/irods/request/accept/123e4567-e89b-12d3-a456-426655440001",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.accept_deletion_request(request_obj) == 1


def test_sodar_api_get_landingzone_retrieve(requests_mock, sodar_api_instance):
    lz = LandingZoneFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/landingzones/api/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(lz),
        status_code=200,
    )
    ret = sodar_api_instance.get_landingzone_retrieve()
    assert ret == lz
    assert sodar_api_instance.project_uuid == lz.project
    assert sodar_api_instance.lz_path == lz.irods_path
    assert sodar_api_instance.assay_uuid == lz.assay


def test_sodar_api_get_landingzone_retrieve_error(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/landingzones/api/retrieve/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_landingzone_retrieve() is None


def test_sodar_api_post_landingzone_create(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    lz = LandingZoneFactory(status="ACTIVE")
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/landingzones/api/create/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(lz),
        status_code=200,
    )
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/landingzones/api/list/123e4567-e89b-12d3-a456-426655440000",
        json=[cattr.unstructure(lz)],
        status_code=200,
    )
    sodar_api_instance.yes = True
    ret = sodar_api_instance.post_landingzone_create()
    assert ret == lz
    assert sodar_api_instance.lz_path == lz.irods_path


def test_sodar_api_post_landingzone_create_error(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    requests_mock.register_uri(
        "POST",
        "https://sodar-staging.bihealth.org/landingzones/api/create/123e4567-e89b-12d3-a456-426655440000",
        status_code=503,
        text="text",
    )
    sodar_api_instance.yes = True
    assert sodar_api_instance.post_landingzone_create() is None


def test_sodar_api_post_landingzone_submit_move(requests_mock, sodar_api_instance):
    lz_uuid = "123e4567-e89b-12d3-a456-426655440001"
    requests_mock.register_uri(
        "POST",
        f"https://sodar-staging.bihealth.org/landingzones/api/submit/move/{lz_uuid}",
        json={"sodar_uuid": lz_uuid},
        status_code=200,
    )
    assert sodar_api_instance.post_landingzone_submit_move(lz_uuid) == lz_uuid


def test_sodar_api_post_landingzone_submit_move_error(requests_mock, sodar_api_instance):
    lz_uuid = "123e4567-e89b-12d3-a456-426655440001"
    requests_mock.register_uri(
        "POST",
        f"https://sodar-staging.bihealth.org/landingzones/api/submit/move/{lz_uuid}",
        status_code=503,
        text="text",
    )
    assert sodar_api_instance.post_landingzone_submit_move(lz_uuid) is None


def test_sodar_api_post_landingzone_submit_validate(requests_mock, sodar_api_instance):
    lz_uuid = "123e4567-e89b-12d3-a456-426655440001"
    requests_mock.register_uri(
        "POST",
        f"https://sodar-staging.bihealth.org/landingzones/api/submit/validate/{lz_uuid}",
        json={"sodar_uuid": lz_uuid},
        status_code=200,
    )
    assert sodar_api_instance.post_landingzone_submit_validate(lz_uuid) == lz_uuid


def test_sodar_api_post_landingzone_submit_validate_error(requests_mock, sodar_api_instance):
    lz_uuid = "123e4567-e89b-12d3-a456-426655440001"
    requests_mock.register_uri(
        "POST",
        f"https://sodar-staging.bihealth.org/landingzones/api/submit/validate/{lz_uuid}",
        status_code=503,
        text="text",
    )
    assert sodar_api_instance.post_landingzone_submit_validate(lz_uuid) is None


def test_sodar_api_get_assay_from_uuid(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    study = list(investigation.studies.values())[0]
    assay = list(study.assays.values())[0]
    sodar_api_instance.assay_uuid = assay.sodar_uuid
    ret_assay, ret_study = sodar_api_instance.get_assay_from_uuid()
    assert ret_assay == assay
    assert ret_study == study


def test_sodar_api_get_assay_from_uuid_no_assay_uuid(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    sodar_api_instance.yes = True
    ret_assay, ret_study = sodar_api_instance.get_assay_from_uuid()
    study = list(investigation.studies.values())[0]
    assert ret_study == study
    assert ret_assay in study.assays.values()


def test_sodar_api_get_assay_from_uuid_not_found(requests_mock, sodar_api_instance):
    investigation = InvestigationFactory()
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        json=cattr.unstructure(investigation),
        status_code=200,
    )
    sodar_api_instance.assay_uuid = "does-not-exist"
    from cubi_tk.exceptions import ParameterException

    with pytest.raises(ParameterException):
        sodar_api_instance.get_assay_from_uuid()


def test_sodar_api_get_assay_from_uuid_no_investigation(requests_mock, sodar_api_instance):
    requests_mock.register_uri(
        "GET",
        "https://sodar-staging.bihealth.org/samplesheets/api/investigation/retrieve/123e4567-e89b-12d3-a456-426655440000",
        status_code=500,
        text="text",
    )
    assert sodar_api_instance.get_assay_from_uuid() == (None, None)


def test_sodar_api_load_toml_config_missing(sodar_api_instance, fs):
    assert sodar_api_instance.load_toml_config(None) is None


def test_sodar_api_load_toml_config_explicit_path(sodar_api_instance, mock_toml_config, fs):
    fs.create_file("/tmp/my_config.toml", contents=mock_toml_config)
    config = sodar_api_instance.load_toml_config("/tmp/my_config.toml")
    assert config["global"]["sodar_api_token"] == "token123"
