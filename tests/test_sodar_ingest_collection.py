"""Tests for ``cubi_tk.sodar.ingest``."""

from argparse import ArgumentParser
from pathlib import Path
from unittest.mock import MagicMock, PropertyMock, call, patch

from cubi_tk.parsers import get_sodar_ingest_parser
import pytest

from cubi_tk.__main__ import main, setup_argparse
from cubi_tk.sodar.ingest_collection import SodarIngestCollection
from cubi_tk.irods_common import TransferJob

from .conftest import my_get_lz_info, my_iRODS_transfer


def test_run_sodar_ingest_collection_help(capsys):
    parser, _subparsers = setup_argparse()
    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-collection", "--help"])

    assert e.value.code == 0

    res = capsys.readouterr()
    assert res.out
    assert not res.err


def test_run_sodar_ingest_collection_nothing(capsys):
    parser, _subparsers = setup_argparse()

    with pytest.raises(SystemExit) as e:
        parser.parse_args(["sodar", "ingest-collection"])

    assert e.value.code == 2

    res = capsys.readouterr()
    assert not res.out
    assert res.err


@pytest.fixture
def ingest(mocker, fs):
    fs.create_dir(Path.home().joinpath(".irods"))
    fs.create_file(Path.home().joinpath(".irods", "irods_environment.json"))

    mocker.patch(
        "cubi_tk.sodar.ingest_collection.SodarIngestCollection._get_lz_info", my_get_lz_info
    )

    argv = [
        "--recursive",
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "--collection",
        "targetCollection",
        "testdir",
        "466ab946-ce6a-4c78-9981-19b79e7bbe86",
    ]

    parser = ArgumentParser(parents=[get_sodar_ingest_parser(include_dest=False)])
    SodarIngestCollection.setup_argparse(parser)
    args = parser.parse_args(argv)

    obj = SodarIngestCollection(args)
    return obj


@pytest.fixture
def target_coll_path():
    return "/irods/dest/targetCollection"


def test_sodar_ingest_collection_build_file_list(fs, caplog):
    class DummyArgs(object):
        pass

    fs.create_symlink("/not_existing", "/broken_link")
    fs.create_symlink("/loop_src", "/loop_src2")
    fs.create_symlink("/loop_src2", "/loop_src")

    args = DummyArgs()
    args.sources = ["broken_link", "not_here", "loop_src", "testdir", "testdir", "file5", "file6"]
    args.recursive = True
    args.exclude = ["file4", "file5"]
    dummy = MagicMock()
    args_mock = PropertyMock(return_value=args)
    type(dummy).args = args_mock

    fs.create_dir("/testdir/subdir")
    fs.create_file("/testdir/file1")
    fs.create_file("/testdir/file1.md5")
    fs.create_file("/testdir/subdir/file2")
    fs.create_file("/file3")
    fs.create_file("/testdir/file4")
    fs.create_file("/file5")
    fs.create_file("/file6")
    fs.create_symlink("/testdir/file3", "/file3")

    paths = SodarIngestCollection.build_file_list(dummy, ".md5")

    # Sources
    assert "File not found: broken_link" in caplog.messages
    assert "File not found: not_here" in caplog.messages
    assert "Symlink loop: loop_src" in caplog.messages

    # Files
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {
        "spath": Path("/testdir/file1.md5"),
        "ipath": Path("file1.md5"),
    } not in paths
    assert {
        "spath": Path("/testdir/subdir/file2"),
        "ipath": Path("subdir/file2"),
    } in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths

    # Re-run without recursive search
    args.recursive = False
    paths = SodarIngestCollection.build_file_list(dummy, ".md5")
    assert {"spath": Path("/testdir/file1"), "ipath": Path("file1")} in paths
    assert {
        "spath": Path("/testdir/file1.md5"),
        "ipath": Path("file1.md5"),
    } not in paths
    assert {
        "spath": Path("/testdir/subdir/file2"),
        "ipath": Path("subdir/file2"),
    } not in paths
    assert {"spath": Path("/testdir/file3"), "ipath": Path("file3")} in paths
    assert {"spath": Path("/testdir/file4"), "ipath": Path("file4")} not in paths
    assert {"spath": Path("file5"), "ipath": Path("file5")} not in paths
    assert {"spath": Path("file6"), "ipath": Path("file6")} in paths


@patch("cubi_tk.sodar.ingest_collection.SodarIngestCollection.build_target_coll")
@patch("cubi_tk.sodar.ingest_collection.SodarIngestCollection.build_file_list")
def test_sodar_ingest_collection_build_jobs(mock_list, mock_target, target_coll_path, ingest, fs):
    paths = [
        {"spath": Path("myfile.csv"), "ipath": Path("dest_dir/myfile.csv")},
        {"spath": Path("folder/file.csv"), "ipath": Path("dest_dir/folder/file.csv")},
    ]
    for path in paths:
        fs.create_file(path["spath"])
    fs.create_file("myfile.csv.md5")
    mock_list.return_value = paths
    mock_target.return_value = target_coll_path

    jobs = ingest.build_jobs(".md5")
    expected_jobs = []

    for p in paths:
        expected_jobs.append(
            TransferJob(
                path_local=str(p["spath"]),
                path_remote=f"{target_coll_path}/{str(p['ipath'])}",
            )
        )
        expected_jobs.append(
            TransferJob(
                path_local=str(p["spath"]) + ".md5",
                path_remote=f"{target_coll_path}/{str(p['ipath']) + '.md5'}",
            )
        )

    assert jobs == sorted(expected_jobs, key=lambda x: x.path_local)


# @patch("cubi_tk.sodar.ingest_collection.iRODSTransfer")
@patch("cubi_tk.common.check_call")
@patch("cubi_tk.sodar_common.iRODSTransfer")
@patch("cubi_tk.sodar_api.requests.get")
@patch("cubi_tk.sodar.ingest_collection.SodarIngestCollection._no_files_found_warning")
@patch("cubi_tk.sodar.ingest_collection.SodarIngestCollection._get_lz_info")
@patch("cubi_tk.common.Value", MagicMock())
def test_sodar_ingest_collection_smoketest(
    mock_lzinfo, mock_filecheck, mockapi, mock_transfer, mock_check_call, fs
):
    mock_filecheck.return_value = 0
    mock_check_call.return_value = 0
    # Setup transfer mocks
    mock_transfer_obj = my_iRODS_transfer()
    mock_transfer.return_value = mock_transfer_obj
    mock_session = MagicMock()
    mock_transfer_obj.session = mock_session

    fs.create_dir("/source/subdir")
    fs.create_dir("/target/coll/")
    fs.create_file("/source/file1")
    fs.create_file("/source/subdir/file2")
    lz_uuid = "f46b4fc3-0927-449d-b725-9ffed231507b"
    argv = [
        "sodar",
        "ingest-collection",
        "--verbose",
        "--sodar-server-url",
        "sodar_server_url",
        "--sodar-api-token",
        "token",
        "--parallel-checksum-jobs",
        "0",
        "--collection",
        "coll",
        "--yes",
        "--recursive",
        "source",
        lz_uuid,
    ]
    # setup lz_info mock
    mock_lzinfo.return_value = lz_uuid, "target"

    # Test for existing irods_environment file is (now) handled by iRODSTransfer, which is mocked
    # fs.create_dir(Path.home().joinpath(".irods"))
    # fs.create_file(Path.home().joinpath(".irods", "irods_environment.json"))

    # Test args no api token
    with pytest.raises(SystemExit):
        argv2 = argv.copy()
        argv2.remove("--sodar-api-token")
        argv2.remove("token")
        main(argv2)

    # Test cancel no invalid LZ
    api_return = {
        "assay": "",
        "config_data": "",
        "configuration": "",
        "date_modified": "",
        "description": "",
        "irods_path": "target",
        "project": "",
        "sodar_uuid": "",
        "status": "ACTIVE",
        "status_locked": "",
        "status_info": "",
        "title": "",
        "user": "",
    }
    mockapi.return_value.status_code = 200
    mockapi.return_value.json = MagicMock(return_value=api_return)

    # proper LZ selection is (now) handled by SodarAPI / SodarIngestBase
    # with pytest.raises(SystemExit):
    #     main(argv)
    #     mockapi.assert_called_with(
    #         sodar_url="sodar_server_url", sodar_api_token="token", landingzone_uuid=lz_uuid
    #     )

    # Test [cancel if] no files to transfer
    # > _no_files_found_warning function would raise sys.exit(1) if not mocked
    argv2 = argv.copy()
    argv2[-2] = "empty"
    main(argv2)
    mock_filecheck.assert_called_with([])

    # Test user input for subcollection
    class DummyColl(object):
        pass

    dcoll = DummyColl()
    dcoll.subcollections = []
    mocki = MagicMock()  # returned by the session context manager
    mock_session.__enter__.return_value = mocki
    mocki.collections.get.return_value = dcoll
    argv2 = argv.copy()
    argv2.remove("--collection")
    argv2.remove("coll")
    argv2.remove("--yes")

    with patch("builtins.input", side_effect=["a", "100", "1"]) as mockinput:
        # Test for no subcollections
        main(argv2)
        mockinput.assert_not_called()

        # Test for 1 subcollection
        dcoll.subcollections = [
            DummyColl(),
        ]
        dcoll.subcollections[0].name = "coll"
        main(argv2)
        assert mockinput.call_count == 3
        mock_filecheck.assert_called()

    # Test upload logic
    mock_filecheck.reset_mock()
    main(argv)
    assert call.collections.get("target") in mocki.mock_calls

    expected_jobs = [
        TransferJob(path_local="/source/file1", path_remote="target/coll/file1"),
        TransferJob(path_local="/source/file1.md5", path_remote="target/coll/file1.md5"),
        TransferJob(path_local="/source/subdir/file2", path_remote="target/coll/subdir/file2"),
        TransferJob(
            path_local="/source/subdir/file2.md5", path_remote="target/coll/subdir/file2.md5"
        ),
    ]
    mock_filecheck.assert_called_with(expected_jobs)
