from pathlib import Path
import shutil
from unittest.mock import ANY, MagicMock, patch

import irods.exception
from irods.session import NonAnonymousLoginWithoutPassword
import pytest

from cubi_tk.irods_common import TransferJob, iRODSCommon, iRODSTransfer


@pytest.fixture
def fake_filesystem(fs):
    yield fs


@patch("cubi_tk.irods_common.iRODSSession")
def test_common_init(mocksession):
    assert iRODSCommon().irods_env_path is not None
    assert type(iRODSCommon().ask) is bool


@patch("cubi_tk.irods_common.iRODSSession")
def test_get_irods_error(mocksession):
    e = irods.exception.NetworkException()
    assert iRODSCommon().get_irods_error(e) == "NetworkException"
    e = irods.exception.NetworkException("Connection reset")
    assert iRODSCommon().get_irods_error(e) == "Connection reset"


@patch("getpass.getpass")
@patch("cubi_tk.irods_common.iRODSSession")
def test_check_auth(mocksession, mockpass, fs):
    fs.create_file(".irods/irods_environment.json")
    password = "1234"

    icommon = iRODSCommon()
    mockpass.return_value = password
    with patch.object(icommon, "_init_irods") as mockinit:
        mockinit.side_effect = NonAnonymousLoginWithoutPassword()

        # .irodsA not found, asks for password
        icommon._check_auth()
        mockpass.assert_called()
        mocksession.assert_any_call(irods_env_file=ANY, password=password)

    # .irodsA there, does not ask for password
    mockpass.reset_mock()
    mocksession.reset_mock()
    icommon._check_auth()
    mockpass.assert_not_called()
    mocksession.assert_called_once()


@patch("cubi_tk.irods_common.encode", return_value="it works")
@patch("cubi_tk.irods_common.iRODSSession")
def test_save_irods_token(mocksession, mockencode, fs):
    token = [
        "secure",
    ]
    icommon = iRODSCommon()
    icommon.irods_env_path = Path("testdir/env.json")
    icommon._save_irods_token(token=token)

    assert icommon.irods_env_path.parent.joinpath(".irodsA").exists()
    mockencode.assert_called_with("secure")


@patch("cubi_tk.irods_common.iRODSSession")
def test_init_irods(mocksession, fs):
    fs.create_file(".irods/irods_environment.json")
    fs.create_file(".irods/.irodsA")

    iRODSCommon()._init_irods()
    mocksession.assert_called()


@patch("cubi_tk.irods_common.iRODSSession")
def test_get_irods_sessions(mocksession):
    with iRODSCommon()._get_irods_sessions(count=3) as sessions:
        assert len(sessions) == 3
    with iRODSCommon()._get_irods_sessions(count=-1) as sessions:
        assert len(sessions) == 1


# Test iRODSTransfer #########
@pytest.fixture
def jobs():
    return (
        TransferJob(path_src="myfile.csv", path_dest="dest_dir/myfile.csv", bytes=123),
        TransferJob(path_src="folder/file.csv", path_dest="dest_dir/folder/file.csv", bytes=1024),
    )


@pytest.fixture
def itransfer(jobs):
    with patch("cubi_tk.irods_common.iRODSSession"):
        return iRODSTransfer(jobs)


def test_irods_transfer_init(jobs, itransfer):
    assert itransfer.total_bytes == sum([job.bytes for job in jobs])
    assert itransfer.destinations == [job.path_dest for job in jobs]


def test_irods_transfer_put(fs, itransfer, jobs):
    for job in jobs:
        fs.create_file(job.path_src)
        fs.create_dir(Path(job.path_dest).parent)

    with patch.object(itransfer.session.data_objects, "put", wraps=shutil.copy):
        itransfer.put()

    for job in jobs:
        assert Path(job.path_dest).exists()


def test_irods_transfer_chksum(itransfer):
    with patch.object(itransfer.session.data_objects, "get") as mockget:
        mock_data_object = MagicMock()
        mockget.return_value = mock_data_object
        mock_data_object.checksum = None
        mock_data_object.chksum = MagicMock()

        itransfer.chksum()

        assert mock_data_object.chksum.call_count == len(itransfer.destinations)
        for path in itransfer.destinations:
            mockget.assert_any_call(path)
