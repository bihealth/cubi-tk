from pathlib import Path
from unittest.mock import ANY, MagicMock, call, patch

import irods.exception
import pytest

from cubi_tk.irods_common import TransferJob, iRODSCommon, iRODSTransfer


def test_transfer_job_bytes(fs):
    fs.create_file("test_file", st_size=123)
    assert TransferJob("test_file", "remote/path").bytes == 123
    assert TransferJob("no_file.no", "remote/path").bytes == -1


@patch("cubi_tk.irods_common.iRODSSession")
def test_common_init(mocksession):
    assert iRODSCommon().irods_env_path is not None
    icommon = iRODSCommon(irods_env_path="a/b/c.json")
    assert icommon.irods_env_path == "a/b/c.json"
    assert type(iRODSCommon().ask) is bool
    assert iRODSCommon().session is mocksession.return_value


@patch("cubi_tk.irods_common.iRODSSession")
def test_get_irods_error(mocksession):
    e = irods.exception.NetworkException()
    assert iRODSCommon().get_irods_error(e) == "NetworkException"
    e = irods.exception.NetworkException("Connection reset")
    assert iRODSCommon().get_irods_error(e) == "Connection reset"


@patch("cubi_tk.irods_common.iRODSSession")
def test_init_irods(mocksession, fs):
    fs.create_file(".irods/irods_environment.json")
    fs.create_file(".irods/.irodsA")

    iRODSCommon()._init_irods()
    mocksession.assert_called()


@patch("cubi_tk.irods_common.iRODSCommon._init_irods")
def test_get_irods_sessions(mockinit):
    with iRODSCommon()._get_irods_sessions(count=4) as sessions:
        [s for s in sessions]
    assert mockinit.call_count == 4

    mockinit.reset_mock()
    with iRODSCommon()._get_irods_sessions(count=-1) as sessions:
        [s for s in sessions]
    assert mockinit.call_count == 1


@patch("getpass.getpass")
@patch("cubi_tk.irods_common.iRODSSession")
def test_irods_login(mocksession, mockpass, fs):
    fs.create_file(".irods/irods_environment.json")
    password = "1234"
    icommon = iRODSCommon()
    mockpass.return_value = password

    icommon._irods_login()
    mockpass.assert_called()
    mocksession.assert_any_call(irods_env_file=ANY, password=password)


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


# Test iRODSTransfer #########
@pytest.fixture
def jobs():
    return (
        TransferJob(path_local="myfile.csv", path_remote="dest_dir/myfile.csv", bytes=123),
        TransferJob(
            path_local="folder/file.csv", path_remote="dest_dir/folder/file.csv", bytes=1024
        ),
    )


def test_irods_transfer_init(jobs):
    with patch("cubi_tk.irods_common.iRODSSession"):
        itransfer = iRODSTransfer(jobs=jobs, irods_env_path="a/b/c", ask=True)
        assert itransfer.irods_env_path == "a/b/c"
        assert itransfer.ask is True
        assert itransfer.jobs == jobs
        assert itransfer.size == sum([job.bytes for job in jobs])
        assert itransfer.destinations == [job.path_remote for job in jobs]


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
@patch("cubi_tk.irods_common.iRODSTransfer._create_collections")
def test_irods_transfer_put(mockrecursive, mocksession, jobs):
    mockput = MagicMock()
    mockexists = MagicMock(return_value=True)
    mockobj = MagicMock()
    mockobj.put = mockput
    mockobj.exists = mockexists

    # fit for context management
    mocksession.return_value.__enter__.return_value.data_objects = mockobj
    itransfer = iRODSTransfer(jobs)

    # put
    itransfer.put()
    calls = [call(j.path_local, j.path_remote) for j in jobs]
    mockput.assert_has_calls(calls)

    # recursive
    itransfer.put(recursive=True)
    calls = [call(j) for j in jobs]
    mockrecursive.assert_has_calls(calls)

    # sync
    mockput.reset_mock()
    itransfer.put(sync=True)
    mockput.assert_not_called()
    mockexists.assert_called()


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_create_collections(mocksession, jobs):
    mockcreate = MagicMock()
    mockcoll = MagicMock()
    mockcoll.create = mockcreate
    mocksession.return_value.__enter__.return_value.collections = mockcoll
    itransfer = iRODSTransfer(jobs)

    itransfer._create_collections(itransfer.jobs[1])
    coll_path = str(Path(itransfer.jobs[1].path_remote).parent)
    mockcreate.assert_called_with(coll_path)


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_irods_transfer_chksum(mocksession, jobs):
    mockget = MagicMock()
    mockobj = MagicMock()
    mockobj.get = mockget
    mocksession.return_value.__enter__.return_value.data_objects = mockobj

    mock_data_object = MagicMock()
    mock_data_object.checksum = None
    mock_data_object.chksum = MagicMock()
    mockget.return_value = mock_data_object

    itransfer = iRODSTransfer(jobs)
    itransfer.chksum()

    assert mock_data_object.chksum.call_count == len(itransfer.destinations)
    for path in itransfer.destinations:
        mockget.assert_any_call(path)


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
def test_irods_transfer_get(mocksession, jobs):
    mockget = MagicMock()
    mockobj = MagicMock()
    mockobj.get = mockget
    mocksession.return_value.__enter__.return_value.data_objects = mockobj
    itransfer = iRODSTransfer(jobs)

    mockget.return_value.size = 111
    itransfer.get()

    for job in jobs:
        # size check
        mockget.assert_any_call(job.path_remote)
        # download
        mockget.assert_any_call(job.path_remote, job.path_local)
    assert itransfer.size == 222
