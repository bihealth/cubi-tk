from argparse import Namespace
import os
import pytest
from unittest.mock import patch, MagicMock

from cubi_tk import sodar_api
from cubi_tk.common import GLOBAL_CONFIG_PATH
from cubi_tk.exceptions import ParameterException, SodarAPIException
from tests.conftest import my_sodar_api_export
from tests.factories import InvestigationFactory


@pytest.fixture
def sodar_api_args():
    return {
        "config": None,
        "sodar_server_url": "https://sodar.bihealth.org/",
        "sodar_api_token": "token123",
        "project_uuid": "123e4567-e89b-12d3-a456-426655440000",
    }


@pytest.fixture
def sodar_api_instance(sodar_api_args):
    return sodar_api.SodarAPI(Namespace(**sodar_api_args))


def test_sodar_api_check_args(sodar_api_args, mock_toml_config, fs):
    # Check args is automatically called in __init__, so we only need to create instances for testing
    args = sodar_api_args.copy()

    # Successful baseline creation
    sodar_api.SodarAPI(Namespace(**args))

    # No toml config available, fail if any value is not given, or malformed
    args["sodar_server_url"] = ""
    with pytest.raises(ParameterException):
        sodar_api.SodarAPI(Namespace(**args))
    args["sodar_server_url"] = "https://sodar.bihealth.org/"
    args["sodar_api_token"] = ""
    with pytest.raises(ParameterException):
        sodar_api.SodarAPI(Namespace(**args))
    args["sodar_api_token"] = "token"
    args["project_uuid"] = "not a uuid"
    with pytest.raises(ParameterException):
        sodar_api.SodarAPI(Namespace(**args))

    # With toml config available, only project_uuid is required
    fs.create_file(os.path.expanduser(GLOBAL_CONFIG_PATH), contents=mock_toml_config)
    sodar_api.SodarAPI(Namespace(config = None, sodar_server_url="", sodar_api_token="", project_uuid="123e4567-e89b-12d3-a456-426655440000"))


@patch("cubi_tk.sodar_api.requests.get")
@patch("cubi_tk.sodar_api.requests.post")
def test_sodar_api_api_call(mock_post, mock_get, sodar_api_instance):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json = MagicMock(return_value={"test": "test"})

    # Test simple request
    out = sodar_api_instance._api_call("samplesheet", "test")
    mock_get.assert_called_with(
        "https://sodar.bihealth.org/samplesheet/api/test/123e4567-e89b-12d3-a456-426655440000",
        headers={"Authorization": "token token123"},
    )
    assert out == {"test": "test"}

    # Test request with params
    # FIXME: also test proper URL encoding of params?
    out = sodar_api_instance._api_call("samplesheet", "test", params={"test": "test"})
    mock_get.assert_called_with(
        "https://sodar.bihealth.org/samplesheet/api/test/123e4567-e89b-12d3-a456-426655440000?test=test",
        headers={"Authorization": "token token123"},
    )

    # Test request with error
    mock_get.return_value.status_code = 123
    with pytest.raises(SodarAPIException):
        out = sodar_api_instance._api_call("samplesheet", "test/action")

    # Test post request with extra data
    mock_post.return_value.status_code = 200
    out = sodar_api_instance._api_call(
        "landingzones", "fake/upload", method="post", data={"test": "test2"}
    )
    mock_post.assert_called_once_with(
        "https://sodar.bihealth.org/landingzones/api/fake/upload/123e4567-e89b-12d3-a456-426655440000",
        headers={"Authorization": "token token123"},
        files=None,
        data={"test": "test2"},
    )

@patch("cubi_tk.sodar_api.api.samplesheet.retrieve")
@patch("cubi_tk.sodar_api.SodarAPI._api_call")
def test_sodar_api_get_ISA_samplesheet(mock_api_call, mock_samplesheet_retrieve, sodar_api_instance):
    mock_api_call.return_value = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}},
        "date_modified": "2021-09-01T12:00:00Z",
    }
    expected = {
        "investigation": {"filename": "i_Investigation.txt", "content": ""},
        "study": {"filename": "s_Study_0.txt", "content": ""},
        "assay": {"filename": "a_name_0", "content": ""},
    }
    assert expected == sodar_api_instance.get_ISA_samplesheet()
    mock_api_call.return_value = {
        "investigation": {"path": "i_Investigation.txt", "tsv": ""},
        "studies": {"s_Study_0.txt": {"tsv": ""}, "s_Study_1.txt": {"tsv": ""}},
        "assays": {"a_name_0": {"tsv": ""}, "a_name_1": {"tsv": ""}},
        "date_modified": "2021-09-01T12:00:00Z",
    }
    expected = {
        "investigation": {"filename": "i_Investigation.txt", "content": ""},
        "study": {"filename": "s_Study_0.txt", "content": ""},
        "assay": {"filename": "a_name_0", "content": ""},
    }
    mock_samplesheet_retrieve.return_value = InvestigationFactory()
    assert expected == sodar_api_instance.get_ISA_samplesheet()
    
