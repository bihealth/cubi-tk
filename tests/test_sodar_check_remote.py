"""Tests for ``cubi_tk.snappy.check_remote``."""

import hashlib
from pathlib import Path as RealPath
from pyfakefs.fake_pathlib import FakePath
import re

import pytest
from unittest.mock import MagicMock, patch

from cubi_tk.sodar.check_remote import (
    FileComparisonChecker,
    FileDataObject,
    FindLocalChecksumFiles,
)
from cubi_tk.__main__ import main
from cubi_tk.exceptions import FileChecksumMismatchException

from .helpers import createIrodsDataObject as IrodsDataObject


# Monkey-Patch Path comparison with Pyfake-fs
def path_eq(self, other):
    if isinstance(other, (RealPath, FakePath)):
        return str(self) == str(other)
    return super(RealPath, self).__eq__(other)


RealPath.__eq__ = path_eq


@pytest.fixture()
def local_file_objects():
    test_dir_path = RealPath("data") / "sodar_check_remote"
    all_files = {
        test_dir_path / "test1": [
            FileDataObject(
                "test1.txt",
                str(test_dir_path / "test1/test1.txt"),
                "fa029a7f2a3ca5a03fe682d3b77c7f0d",
            )
        ],
        test_dir_path / "test2": [
            FileDataObject(
                "test2.txt",
                str(test_dir_path / "test2/test2.txt"),
                "856babf68edfd13e2fd019df330e11c5",
            )
        ],
        test_dir_path / "test3": [
            FileDataObject(
                "test3.txt",
                str(test_dir_path / "test3/test3.txt"),
                "d6618babc17b25b73eb0d0a68947babd",
            )
        ],
    }
    return all_files


def md5_file_checksum(str_value):
    full_str = str_value + "\n"
    the_hash = hashlib.md5(full_str.encode("utf-8"))
    return the_hash.hexdigest()


@pytest.fixture()
def fake_file_setup(local_file_objects, fs):
    fs.create_dir("data/sodar_check_remote")
    for dir, objs in local_file_objects.items():
        fs.create_dir(dir)
        content = RealPath(dir).name.capitalize()
        # Setup intentional mismatch
        if dir == RealPath("data/sodar_check_remote") / "test3":
            content = "Tset3"
        fs.create_file(objs[0].file_path, contents=content + "\n")
        fs.create_file(
            objs[0].file_path + ".md5",
            contents=f"{objs[0].file_checksum}  {RealPath(objs[0].file_path).name}",
        )
    # Add file without checksum
    fs.create_file("data/sodar_check_remote/test2/testx.txt")
    # Add checksum file from different hash_scheme
    fs.create_file(
        "data/sodar_check_remote/test2/test2.txt.sha256", contents="0123456789  test2.txt"
    )


@pytest.fixture()
def irods_file_objects():
    return {
        "test1.txt": [
            IrodsDataObject(
                "test1.txt",
                "/test1/test1.txt",
                "fa029a7f2a3ca5a03fe682d3b77c7f0d",
                3 * ["fa029a7f2a3ca5a03fe682d3b77c7f0d"],
            )
        ],
        "test2.txt": [
            IrodsDataObject(
                "test2.txt",
                "/test2/test2.txt",
                "856babf68edfd13e2fd019df330e11c5",
                3 * ["856babf68edfd13e2fd019df330e11c5"],
            )
        ],
        "test3.txt": [
            IrodsDataObject(
                "test3.txt",
                "/test3/test3.txt",
                "0f034ea35fde3fca41d71cbcb13ee659",
                3 * ["0f034ea35fde3fca41d71cbcb13ee659"],
            )
        ],
        "test5.txt": [
            IrodsDataObject("test5.txt", "/test5/test5.txt", "abcdefgh", 3 * ["abcdefgh"])
        ],
    }


def test_findlocalmd5_run(local_file_objects, fake_file_setup, fs, caplog):
    # Run 3 simple & 1 combined test
    # test1: single file with md5sum in 1 folder
    # test2: folder has 1 file with md5sum & one without
    # test3: empty folder
    # combined: all folder + 1 extra
    test_dir_path = RealPath("data") / "sodar_check_remote"
    expected_all = local_file_objects.copy()
    expected_1 = {k: v for k, v in local_file_objects.items() if str(k).endswith("1")}
    expected_2 = {k: v for k, v in local_file_objects.items() if str(k).endswith("2")}

    actual_1 = FindLocalChecksumFiles(
        test_dir_path / "test1", hash_scheme="MD5", recheck_checksum=False
    ).run()
    assert actual_1 == expected_1

    actual_2 = FindLocalChecksumFiles(
        test_dir_path / "test2", hash_scheme="MD5", recheck_checksum=False
    ).run()
    assert actual_2 == expected_2

    empty_dir = test_dir_path / "empty_test"
    empty_dir.mkdir()
    actual_empty = FindLocalChecksumFiles(
        empty_dir, hash_scheme="MD5", recheck_checksum=False
    ).run()
    assert actual_empty == {}
    empty_dir.rmdir()

    actual = FindLocalChecksumFiles(test_dir_path, hash_scheme="MD5", recheck_checksum=False).run()
    for dirname, filelist in expected_all.items():
        entry = actual.pop(dirname)
        assert entry == filelist
    assert actual == {}

    # Additional test with mis-formatted checksum file and orphaned checksum file
    caplog.clear()
    caplog.set_level("WARNING")
    fs.create_file("data/sodar_check_remote/test2/testA.txt.md5", contents="0123456789  testA.txt")
    fs.create_file("data/sodar_check_remote/test2/testB.txt", contents="123")
    fs.create_file("data/sodar_check_remote/test2/testB.txt.md5", contents="123456")
    actual_2 = FindLocalChecksumFiles(
        test_dir_path / "test2",
        hash_scheme="MD5",
        recheck_checksum=False,
        skip_unreadable_checksum=True,
    ).run()
    assert caplog.messages == [
        "Ignoring orphaned local checksum file: data/sodar_check_remote/test2/testA.txt.md5.\nExpected associated file not found: data/sodar_check_remote/test2/testA.txt",
        "Ignoring misformatted local checksum file: data/sodar_check_remote/test2/testB.txt.md5 (content: 123456)",
    ]
    assert actual_2 == expected_2
    # Raise error without skip_unreadable_checksum=True
    with pytest.raises(
        FileChecksumMismatchException,
        match="Local checksum file is not formatted correctly: data/sodar_check_remote/test2/testB.txt.md5",
    ):
        FindLocalChecksumFiles(
            test_dir_path / "test2", hash_scheme="MD5", recheck_checksum=False
        ).run()


def test_filecomparisoncheck_compare_local_and_remote_files(
    irods_file_objects, local_file_objects, fake_file_setup
):
    """Tests FileComparisonChecker.compare_local_and_remote_files()"""
    test_dir_path = RealPath("data") / "sodar_check_remote"
    # Setup extra local FileDataObjects:
    local_file_objects.update(
        {
            test_dir_path / "test4": [
                FileDataObject("test4.txt", str(test_dir_path / "test4.txt"), "abcde")
            ]
        }
    )
    # Expectations including md5 checks (test3 has different md5 sums local & remote)
    expected_both = {k: v for k, v in local_file_objects.items() if str(k)[-1] in "12"}
    expected_local = {k: v for k, v in local_file_objects.items() if str(k)[-1] in "34"}
    expected_remote = {
        "/test3": [FileDataObject("test3.txt", "/test3", "0f034ea35fde3fca41d71cbcb13ee659")],
        "/test5": [FileDataObject("test5.txt", "/test5", "abcdefgh")],
    }
    actual_both, actual_local, actual_remote = FileComparisonChecker.compare_local_and_remote_files(
        local_file_objects, irods_file_objects
    )
    assert expected_both == actual_both
    assert expected_local == actual_local
    assert expected_remote == actual_remote
    # Check matching by filename only (test3 moves to expected_both)
    expected_both = {k: v for k, v in local_file_objects.items() if str(k)[-1] in "123"}
    expected_local = {k: v for k, v in local_file_objects.items() if str(k)[-1] in "4"}
    expected_remote.pop("/test3")
    actual_both, actual_local, actual_remote = FileComparisonChecker.compare_local_and_remote_files(
        local_file_objects, irods_file_objects, filenames_only=True
    )
    assert expected_both == actual_both
    assert expected_local == actual_local
    assert expected_remote == actual_remote


# Smoketest, including regex and out
@patch("cubi_tk.sodar.check_remote.RetrieveSodarCollection")
def test_sodar_check_remote(mock_rsc, irods_file_objects, fake_file_setup, capsys, fs):  # noqa: C901
    mock_rsc.return_value = MagicMock(
        irods_hash_scheme="MD5",
        perform=MagicMock(return_value=irods_file_objects),
        get_assay_irods_path=MagicMock(return_value="/"),
    )
    argv = [
        "sodar",
        "check-remote",
        "-p",
        "data/sodar_check_remote",
        "DUMMY-UUID",
    ]

    def get_checksum():
        yield from ("fa029a7f", "856babf6", "d6618bab", "0f034ea3", "abcdefgh")

    def get_expected(both=(1, 2), local=(3,), remote=(3, 5), incl_checksums=False):  # noqa: C901
        checksums = get_checksum()
        expected = []
        if both:
            expected += ["Files found BOTH locally and remotely:"]
            for n in both:
                expected += [f"data/sodar_check_remote/test{n}:"]
                expected += [
                    f"    test{n}.txt" + (f"  ({next(checksums)})" if incl_checksums else "")
                ]
        elif both is not None:
            expected += ["No file was found both locally and remotely."]
        if both is not None and local is not None:
            expected += ["-" * 25]
        if local:
            expected += ["Files found ONLY LOCALLY:"]
            for n in local:
                expected += [f"data/sodar_check_remote/test{n}:"]
                expected += [
                    f"    test{n}.txt" + (f"  ({next(checksums)})" if incl_checksums else "")
                ]
        elif local is not None:
            expected += ["No file found only locally."]
        if remote is not None and (local is not None or both is not None):
            expected += ["-" * 25]
        if remote:
            expected += ["Files found ONLY REMOTELY:"]
            for n in remote:
                expected += [f"test{n}:"]
                expected += [
                    f"    test{n}.txt" + (f"  ({next(checksums)})" if incl_checksums else "")
                ]
        elif remote is not None:
            expected += ["No file found only remotely."]
        return expected

    main(argv)
    # Note: this could just be `capsys.readouterr().out.split('\n')` if logs were written to stderr
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected()

    # (Re)Test filename only opyion
    main(argv + ["--filename-only"])
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected((1, 2, 3), False, (5,))

    # Test regex selection option
    main(argv + ["--file-selection-regex", "test[123].txt$"])
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected((1, 2), (3,), (3,))

    # Test selection of output categories
    main(argv + ["--report-categories", "both", "local-only"])
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected((1, 2), (3,), None)

    # Test inclusion of md5 in output
    main(argv + ["--report-checksums"])
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected((1, 2), (3,), (3, 5), incl_checksums=True)

    # Note: all checksums are correct
    main(argv + ["--recheck-checksum", "--report-checksums"])
    output = [
        l
        for l in capsys.readouterr().out.split("\n")
        if l and not re.match(r"(W -|I -|S -| \.\.\.)", l)
    ]
    assert output == get_expected((1, 2), (3,), (3, 5), incl_checksums=True)
    # Invalidate a local checksum
    file = fs.get_object("data/sodar_check_remote/test3/test3.txt.md5")
    file.set_contents("0" * 32 + "  test3.txt\n")
    expected_msg = f"Wrong checksum recorded for file: data/sodar_check_remote/test3/test3.txt. Recorded checksum: {'0' * 32}, excepted checksum: d6618babc17b25b73eb0d0a68947babd."
    with pytest.raises(FileChecksumMismatchException):
        main(argv + ["--recheck-checksum", "--report-checksums"])
    assert expected_msg in capsys.readouterr().out.split("\n")[-2]
