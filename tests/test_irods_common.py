from pathlib import Path
import shutil
from unittest.mock import MagicMock, PropertyMock, patch

import irods.exception
from irods.session import iRODSSession
import pytest

from cubi_tk.irods_common import (
    TransferJob,
    get_irods_error,
    init_irods,
    iRODSTransfer,
    save_irods_token,
)


@pytest.fixture
def fake_filesystem(fs):
    yield fs


@pytest.fixture
def jobs():
    return (
        TransferJob(path_src="myfile.csv", path_dest="dest_dir/myfile.csv", bytes=123),
        TransferJob(path_src="folder/file.csv", path_dest="dest_dir/folder/file.csv", bytes=1024),
    )


@pytest.fixture
def itransfer(jobs):
    session = iRODSSession(
        irods_host="localhost",
        irods_port=1247,
        irods_user_name="pytest",
        irods_zone_name="pytest",
    )
    return iRODSTransfer(session, jobs)


def test_get_irods_error():
    e = irods.exception.NetworkException()
    assert get_irods_error(e) == "NetworkException"
    e = irods.exception.NetworkException("Connection reset")
    assert get_irods_error(e) == "Connection reset"


@patch("cubi_tk.irods_common.iRODSSession")
@patch("getpass.getpass")
def test_init_irods(mockpass, mocksession, fs):
    ienv = Path(".irods/irods_environment.json")
    password = "1234"

    # .irodsA not found, asks for password
    mockpass.return_value = password
    init_irods(ienv)
    mockpass.assert_called()
    mocksession.assert_called_with(irods_env_file=ienv, password=password)

    # .irodsA there, does not ask for password
    fs.create_file(".irods/.irodsA")
    mockpass.reset_mock()
    init_irods(ienv)
    mockpass.assert_not_called()
    mocksession.assert_called_with(irods_env_file=ienv)


@patch("cubi_tk.irods_common.encode", return_value="it works")
def test_write_token(mockencode, fs):
    ienv = Path(".irods/irods_environment.json")

    mocksession = MagicMock()
    pam_pw = PropertyMock(return_value=["secure"])
    type(mocksession).pam_pw_negotiated = pam_pw

    save_irods_token(mocksession, ienv)
    assert ienv.parent.joinpath(".irodsA").exists()
    mockencode.assert_called_with("secure")


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
    with patch.object(itransfer.session.data_objects, "get") as mock:
        itransfer.chksum()

        for path in itransfer.destinations:
            mock.assert_any_call(path)
