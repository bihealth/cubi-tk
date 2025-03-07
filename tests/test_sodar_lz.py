"""``Tests for sodar_lz functions``"""

from argparse import ArgumentParser
from unittest.mock import patch

from cubi_tk.parsers import get_sodar_parser
from cubi_tk.sodar.lz_validate import ValidateLandingZoneCommand


@patch("cubi_tk.sodar.lz_validate.api.landingzone.submit_validate")
def test_validate(mockapi, caplog):
    mockapi.return_value = {"a": 1, "b": 2}
    argv = [
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "u-u-i-d",
    ]

    sodar_parser = get_sodar_parser()
    parser = ArgumentParser(parents=[sodar_parser])
    ValidateLandingZoneCommand.setup_argparse(parser)

    # No format string
    args = parser.parse_args(argv)
    ValidateLandingZoneCommand(args).execute()
    mockapi.assert_called_with(
        sodar_url="sodar_server_url", sodar_api_token="token", landingzone_uuid="u-u-i-d"
    )
    assert '{"a": 1, "b": 2}' in caplog.messages

    # With format string
    argv.insert(-1, "--format")
    argv.insert(-1, "%(a)s")
    args = parser.parse_args(argv)
    ValidateLandingZoneCommand(args).execute()
    assert "1" in caplog.messages
