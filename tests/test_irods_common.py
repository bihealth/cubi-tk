import os
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import irods.exception
from irods.keywords import FORCE_FLAG_KW
import pytest

from cubi_tk.irods_common import (
    TransferJob,
    iRODSCommon,
    iRODSRetrieveCollection,
    iRODSTransfer,
)


def test_transfer_job_bytes(fs):
    fs.create_file("test_file", st_size=123)
    assert TransferJob("test_file", "remote/path").bytes == 123
    assert TransferJob("no_file.no", "remote/path").bytes == -1


@patch("cubi_tk.irods_common.iRODSSession")
def test_common_init(mocksession):
    assert iRODSCommon().irods_env_path is not None
    icommon = iRODSCommon(irods_env_path="a/b/c.json")
    assert icommon.irods_env_path == Path("a/b/c.json")
    assert type(iRODSCommon().ask) is bool
    assert iRODSCommon().session is mocksession.return_value


@patch("cubi_tk.irods_common.iRODSSession")
def test_get_irods_error(mocksession):
    e = irods.exception.NetworkException()
    assert iRODSCommon().get_irods_error(e) == "NetworkException"
    e = irods.exception.NetworkException("Connection reset")
    assert iRODSCommon().get_irods_error(e) == "Connection reset"


@pytest.fixture
def irods_env_file(fs):
    mockirodsjson = """
    {
        "irods_host": "host",
        "irods_port": 1234,
        "irods_default_hash_scheme": "MD5"
    }
    """
    env_path = os.path.expanduser("~/.irods/irods_environment.json")
    fs.create_file(env_path, contents=mockirodsjson)
    return env_path


@patch("cubi_tk.irods_common.iRODSSession")
def test_init_irods(mocksession, fs, irods_env_file):
    fs.create_file(str(Path(irods_env_file).parent / ".irodsA"))

    iRODSCommon()._init_irods()
    mocksession.assert_called()


@patch("getpass.getpass")
@patch("cubi_tk.irods_common.write_pam_irodsA_file")
def test_check_and_gen_irods_files_creates_irodsA(mock_write_pam, mockpass, fs, irods_env_file):
    password = "1234"
    icommon = iRODSCommon(ask=True)
    mockpass.return_value = password
    # only irodsA file exists but no irods_profile, so profile should be created and irodsA backed up and restored
    fs.create_file(str(Path(irods_env_file).parent / ".irodsA"), contents="""test""")
    irods_a_path = Path(irods_env_file).parent / ".irodsA"
    mock_write_pam.side_effect = lambda *args, **kwargs: irods_a_path.write_text("test_2")
    icommon._check_and_gen_irods_files()

    mockpass.assert_called()
    mock_write_pam.assert_called_once()
    assert irods_a_path.exists()
    assert Path(Path(irods_env_file).parent / ".irodsA_global").exists()
    assert Path(Path(irods_env_file).parent / ".irodsA_backup").exists()
    del icommon  # to trigger cleanup of irodsA file
    assert irods_a_path.exists()
    with open(irods_a_path) as f:
        assert f.read() == "test"
    assert not Path(Path(irods_env_file).parent / ".irodsA_backup").exists()


@patch("getpass.getpass")
@patch("cubi_tk.irods_common.write_pam_irodsA_file")
def test_check_and_gen_irods_files_skips_when_irodsA_exists(
    mock_write_pam, mockpass, fs, irods_env_file
):
    fs.create_file(str(Path(irods_env_file).parent / ".irodsA_global"))
    icommon = iRODSCommon(ask=True)

    icommon._check_and_gen_irods_files()

    mockpass.assert_not_called()
    mock_write_pam.assert_not_called()


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
        assert itransfer.irods_env_path == Path("a/b/c")
        assert itransfer.ask is True
        assert itransfer.jobs == jobs
        assert itransfer.size == sum([job.bytes for job in jobs])
        assert itransfer.destinations == [job.path_remote for job in jobs]


@patch("cubi_tk.irods_common.iRODSTransfer._init_irods")
@patch("cubi_tk.irods_common.iRODSTransfer._create_collections")
def test_irods_transfer_put(mock_createcolls, mocksession, jobs):
    mockput = MagicMock()
    mockexists = MagicMock(return_value=False)
    mockobj = MagicMock()
    mockobj.put = mockput
    mockobj.exists = mockexists
    mockobj.get.return_value = MagicMock(size=123)

    # fit for context management
    mocksession.return_value.__enter__.return_value.data_objects = mockobj
    itransfer = iRODSTransfer(jobs)

    # expected calls
    calls_no_ov = [call(j.path_local, j.path_remote) for j in jobs]
    calls_w_ov = [call(j.path_local, j.path_remote, forceFlag=None) for j in jobs]
    calls_sync = [calls_w_ov[1]]

    # put, no options, no remote files
    itransfer.put()
    mockput.assert_has_calls(calls_no_ov)

    # recursive
    itransfer.put(recursive=True)
    calls = [call(j) for j in jobs]
    mock_createcolls.assert_has_calls(calls)

    # overwrite behaviour with existing files
    mockexists.return_value = True
    # overwrite: sync (w/ exiting files)
    mockput.reset_mock()
    itransfer.put(overwrite="sync")
    mockput.assert_has_calls(calls_sync)
    # overwrite: always
    mockput.reset_mock()
    itransfer.put(overwrite="always")
    mockput.assert_has_calls(calls_w_ov)
    # overwrite: never
    mockput.reset_mock()
    itransfer.put(overwrite="never")
    mockput.assert_not_called()
    # overwrite: ask
    mockput.reset_mock()
    itransfer.ask = True
    with patch("builtins.input", side_effect=["y", "y", "n"]):
        itransfer.put(overwrite="ask")
        mockput.assert_has_calls([calls_w_ov[0]])


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
def test_irods_transfer_get(mocksession, jobs, fs):
    mockget = MagicMock()
    mockget.return_value.size = 123
    mockobj = MagicMock()
    mockobj.get = mockget
    mocksession.return_value.__enter__.return_value.data_objects = mockobj

    itransfer = iRODSTransfer(jobs)

    # Call args
    base_calls = [
        # Size calculation for all jobs
        call(jobs[0].path_remote),
        call(jobs[1].path_remote),
    ]
    job1_size = call(jobs[0].path_remote)
    job1_write = call(jobs[0].path_remote, jobs[0].path_local)
    job1_overwrite = call(jobs[0].path_remote, jobs[0].path_local, **{FORCE_FLAG_KW: None})
    job2_size = call(jobs[1].path_remote)
    job2_write = call(jobs[1].path_remote, jobs[1].path_local)
    job2_overwrite = call(jobs[1].path_remote, jobs[1].path_local, **{FORCE_FLAG_KW: None})

    # No local files, no overwrite
    # each job writes, then checks size for counter update
    calls = base_calls + [job1_write, job1_size, job2_write, job2_size]
    itransfer.get(overwrite="never")
    mockget.assert_has_calls(calls)
    # Sync & overwrite behave the same, since they don't see a local file
    mockget.reset_mock()
    itransfer.get()
    mockget.assert_has_calls(calls)
    mockget.reset_mock()
    itransfer.get(overwrite="always")
    mockget.assert_has_calls(calls)

    fs.create_file(jobs[1].path_local, st_size=jobs[1].bytes)
    # With one local file & no overwrite: job2 has no data_object.get calls
    calls = base_calls + [job1_write, job1_size]
    mockget.reset_mock()
    itransfer.get(overwrite="never")
    mockget.assert_has_calls(calls)
    # Sync will check the local fail and (in this case) overwrite, the update counter size
    calls = base_calls + [job1_write, job1_size, job2_size, job2_overwrite, job2_size]
    mockget.reset_mock()
    itransfer.get()
    mockget.assert_has_calls(calls)
    # Overwrite similar, only without the extra size check call
    calls = base_calls + [job1_write, job1_size, job2_overwrite, job2_size]
    mockget.reset_mock()
    itransfer.get(overwrite="always")
    mockget.assert_has_calls(calls)

    fs.create_file(jobs[0].path_local, st_size=jobs[0].bytes)
    # With both local files & overwrite
    # each job _over_writes, then checks size for counter update
    calls = base_calls + [job1_overwrite, job1_size, job2_overwrite, job2_size]
    mockget.reset_mock()
    itransfer.get(overwrite="always")
    mockget.assert_has_calls(calls)

    # With both local files & sync
    # each job checks size, potentially _over_writes, then checks size for counter update if it was written
    calls = base_calls + [job1_size, job2_size, job2_overwrite, job2_size]
    mockget.reset_mock()
    itransfer.get()
    mockget.assert_has_calls(calls)

    # With both local files & overwrite: ask
    # it can be fine controlled - size check before is never done though (first "y" is for execution)
    itransfer.ask = True
    mockget.reset_mock()
    with patch("builtins.input", side_effect=["y", "y", "n"]):
        itransfer.get(overwrite="ask")
        mockget.assert_has_calls(base_calls + [job1_overwrite, job1_size])
    mockget.reset_mock()
    with patch("builtins.input", side_effect=["y", "n", "y"]):
        itransfer.get(overwrite="ask")
        mockget.assert_has_calls(base_calls + [job2_overwrite, job2_size])


# Test iRODSRetrieveCollection #########
# This tests `retrieve_irods_data_objects` and by extension `parse_irods_collection`
# A test for _irods_query would require mocking `session.query` results in a
# way that allows creation of IrodsDataObject instances from those results
@patch("cubi_tk.irods_common.iRODSCommon._init_irods")
@patch("cubi_tk.irods_common.iRODSRetrieveCollection._irods_query")
def test_irods_retrieve_data_objects(mockquery, mocksession):
    # Possible alternative to MagicMocks here:
    # create a fake IrodsDataObject class with a path attribute
    mockobj1 = MagicMock()
    mockobj1.path = "/root/coll1/file1.vcf.gz"
    mockobj1.name = "file1.vcf.gz"
    mockobj2 = MagicMock()
    mockobj2.path = "/root/coll2/file2.vcf.gz"
    mockobj2.name = "file2.vcf.gz"
    mockobj3 = MagicMock()
    mockobj3.path = "/root/coll1/subcol/file1.vcf.gz"
    mockobj3.name = "file1.vcf.gz"

    mockcksum = MagicMock()

    mockquery.return_value = {
        "files": [mockobj1, mockobj2, mockobj3],
        "checksums": {
            "/root/coll1/file1.vcf.gz": mockcksum,
            "/root/coll2/file2.vcf.gz": mockcksum,
            "/root/coll1/subcol/file1.vcf.gz": mockcksum,
        },
    }

    mocksession.collections.get.return_value = "path"

    data_objs = iRODSRetrieveCollection().retrieve_irods_data_objects("/fake/path")

    expected_data_objs = {"file1.vcf.gz": [mockobj1, mockobj3], "file2.vcf.gz": [mockobj2]}

    assert data_objs == expected_data_objs
