from argparse import Namespace
import os

import cattr

import pytest
from unittest.mock import patch, MagicMock

from cubi_tk.api_models import IrodsDataObject
from cubi_tk.sodar_api import GLOBAL_CONFIG_PATH, SodarApi
from cubi_tk.exceptions import SodarApiException
from tests.factories import InvestigationFactory


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
