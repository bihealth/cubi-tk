"""Tests for ``cubi_tk.snappy.check_remote``."""

import pathlib
# import pytest
from cubi_tk.sodar.check_remote import FileComparisonChecker, FindLocalMD5Files, FileDataObject
from cubi_tk.snappy.retrieve_irods_collection import IrodsDataObject


def test_findlocalmd5_run():
    test_dir_path = pathlib.Path(__file__).resolve().parent / "data" / "sodar_check_remote"
    expected = {
        test_dir_path
        / "test1": [
            FileDataObject(
                "test1.txt",
                str(test_dir_path / "test1/test1.txt"),
                "fa029a7f2a3ca5a03fe682d3b77c7f0d",
            )
        ],
        test_dir_path
        / "test2": [
            FileDataObject(
                "test2.txt",
                str(test_dir_path / "test2/test2.txt"),
                "856babf68edfd13e2fd019df330e11c5",
            )
        ],
        test_dir_path
        / "test3": [
            FileDataObject(
                "test3.txt",
                str(test_dir_path / "test3/test3.txt"),
                "d6618babc17b25b73eb0d0a68947babd",
            )
        ],
    }
    actual = FindLocalMD5Files(test_dir_path, recheck_md5=False).run()
    assert all(expected[dirname] == filelist for dirname, filelist in actual.items())
    # Test any exceptions ?
    # -> should wrong  md5 give warning or error ?

# test_findlocalmd5_run()


def test_filecomparisoncheck_run():
    test_dir_path = pathlib.Path(__file__).resolve().parent / "data" / "sodar_check_remote"
    # Setup (remote) IrodsDataObjects
    remote_dict = {
        "test1.txt": [
            IrodsDataObject(
                "test1.txt", "test1/test1.txt", "fa029a7f2a3ca5a03fe682d3b77c7f0d", 3 * [None]
            )
        ],
        "test2.txt": [
            IrodsDataObject(
                "test2.txt", "test2/test2.txt", "856babf68edfd13e2fd019df330e11c5", 3 * [None]
            )
        ],
        "test3.txt": [
            IrodsDataObject(
                "test3.txt", "test3/test3.txt", "0f034ea35fde3fca41d71cbcb13ee659", 3 * [None]
            )
        ],
        "test5.txt": [IrodsDataObject("test5.txt", "test5/test5.txt", "abcdefgh", 3 * [None])],
    }
    # Setup (local) FileDataObjects:
    test1 = FileDataObject(
        "test1.txt", str(test_dir_path / "test1/test1.txt"), "fa029a7f2a3ca5a03fe682d3b77c7f0d"
    )
    test2 = FileDataObject(
        "test2.txt", str(test_dir_path / "test2/test2.txt"), "856babf68edfd13e2fd019df330e11c5"
    )
    test3 = FileDataObject(
        "test3.txt", str(test_dir_path / "test3/test3.txt"), "d6618babc17b25b73eb0d0a68947babd"
    )
    test4 = FileDataObject("test4.txt", str(test_dir_path / "test4.txt"), "abcde")
    local_dict = {
        test_dir_path / "test1": [test1],
        test_dir_path / "test2": [test2],
        test_dir_path / "test3": [test3],
        test_dir_path / "test4": [test4],
    }
    # Expectations including md5 checks (test3 has different md5 sums local & remote)
    expected_both = {test_dir_path / "test1": [test1], test_dir_path / "test2": [test2]}
    expected_local = {test_dir_path / "test3": [test3], test_dir_path / "test4": [test4]}
    expected_remote = {
        "test3": [FileDataObject("test3.txt", "test3", "0f034ea35fde3fca41d71cbcb13ee659")],
        "test5": [FileDataObject("test5.txt", "test5", "abcdefgh")],
    }
    actual_both, actual_local, actual_remote = FileComparisonChecker.compare_local_and_remote_files(
        local_dict, remote_dict
    )
    assert expected_both == actual_both
    assert expected_local == actual_local
    assert expected_remote == actual_remote
    # Check matching by filename only (test3 moves to expected_both)
    expected_both.update({test_dir_path / "test3": [test3]})
    expected_local.pop(test_dir_path / "test3")
    expected_remote.pop("test3")
    actual_both, actual_local, actual_remote = FileComparisonChecker.compare_local_and_remote_files(
        local_dict, remote_dict, filenames_only=True
    )
    assert expected_both == actual_both
    assert expected_local == actual_local
    assert expected_remote == actual_remote

# test_filecomparisoncheck_run()
