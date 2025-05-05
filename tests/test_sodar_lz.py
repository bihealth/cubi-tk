"""``Tests for sodar_lz functions``"""

from argparse import ArgumentParser
from unittest.mock import MagicMock, patch

from cubi_tk.parsers import get_sodar_parser
from cubi_tk.sodar.lz_validate import ValidateLandingZoneCommand

@patch("cubi_tk.sodar_api.requests.get")
@patch("cubi_tk.sodar_api.requests.post")
def test_validate(mockapi_post, mockapi_get, caplog):
    mockapi_post.return_value.status_code = 200
    mockapi_post.return_value.json = MagicMock(return_value={"a": 1, "sodar_uuid": "466ab946-ce6a-4c78-9981-19b79e7bbe86"})
    mockapi_get.return_value.status_code = 200
    mockapi_get.return_value.json = MagicMock(return_value={
        "assay": "",
        "config_data": "",
        "configuration": "",
        "date_modified": "",
        "description": "",
        "irods_path": "",
        "project": "",
        "sodar_uuid": "466ab946-ce6a-4c78-9981-19b79e7bbe86",
        "status": "",
        "status_locked" : "",
        "status_info": "",
        "title": "",
        "user":  "",
    })
    argv = [
        "--sodar-server-url",
        "https://sodar.bihealth.org/",
        "--sodar-api-token",
        "token",
        "466ab946-ce6a-4c78-9981-19b79e7bbe86",
    ]

    sodar_parser = get_sodar_parser(with_dest= True, dest_string="landing_zone_uuid", dest_help_string="UUID of Landing Zone to move.")
    parser = ArgumentParser(parents=[sodar_parser])
    ValidateLandingZoneCommand.setup_argparse(parser)

    # No format string
    args = parser.parse_args(argv)
    ValidateLandingZoneCommand(args).execute()
    mockapi_post.assert_called_with(
        'https://sodar.bihealth.org/landingzones/api/submit/validate/466ab946-ce6a-4c78-9981-19b79e7bbe86', headers={'Authorization': 'token token', 'Accept': 'application/vnd.bihealth.sodar.landingzones+json; version=1.0'}, files=None, data=None)

    # TODO: rewrite formattest and caplog check
    # assert '"sodar_uuid": "466ab946-ce6a-4c78-9981-19b79e7bbe86"' in caplog.messages
    # With format string
    # argv.insert(-1, "--format")
    # argv.insert(-1, "%(a)s")
    # args = parser.parse_args(argv)
    # ValidateLandingZoneCommand(args).execute()
    # assert "1" in caplog.messages
