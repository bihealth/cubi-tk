""""Test ``cubi-tk isa-tab add-ped``."""

import shutil
import pathlib

from cubi_tk.__main__ import main


def compare_input_output(path_expected, path_output):
    """Compare ISA files from input and output path."""
    path_expected = pathlib.Path(path_expected)
    path_output = pathlib.Path(path_output)
    for path in path_expected.glob("?_*.txt"):
        with path.open("rt") as expected_f:
            with (path_output / path.name).open("rt") as actual_f:
                assert expected_f.read() == actual_f.read(), "filename = %s" % path.name


BASE_ARGS = [
    "--verbose",
    "isa-tab",
    "add-ped",
    "--batch",
    "3",
    "--library-type",
    "WES",
    "--library-layout",
    "PAIRED",
    "--library-kit",
    "Agilent SureSelect Human All Exon V6r2",
    "--library-kit-catalogue-id",
    "S04380110",
    "--platform",
    "ILLUMINA",
    "--instrument-model",
    "Illumina NextSeq 500",
    "--yes",
]


def test_add_ped_from_scratch(tmpdir):
    """Test adding from scratch."""
    scratch_dir = tmpdir / "scratch"
    path_ped = pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_from_scratch" / "input.ped"
    shutil.copytree(
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_from_scratch"),
        str(scratch_dir),
    )
    argv = BASE_ARGS + [str(scratch_dir / "i_Investigation.txt"), str(path_ped)]

    # Actually exercise code and perform test.
    res = main(argv)

    assert not res

    compare_input_output(
        str(scratch_dir),
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "expected_output"),
    )


def test_add_ped_without_assays(tmpdir):
    """Test updating study and appending to assay."""
    scratch_dir = tmpdir / "scratch"
    path_ped = (
        pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_without_assays" / "input.ped"
    )
    shutil.copytree(
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_without_assays"),
        str(scratch_dir),
    )
    argv = BASE_ARGS + [str(scratch_dir / "i_Investigation.txt"), str(path_ped)]

    # Actually exercise code and perform test.
    res = main(argv)

    assert not res

    compare_input_output(
        str(scratch_dir),
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "expected_output"),
    )


def test_add_ped_just_update(tmpdir):
    """Test updating study and assay."""
    scratch_dir = tmpdir / "scratch"
    path_ped = pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_just_update" / "input.ped"
    shutil.copytree(
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "in_just_update"), str(scratch_dir)
    )
    argv = BASE_ARGS + [str(scratch_dir / "i_Investigation.txt"), str(path_ped)]

    # Actually exercise code and perform test.
    res = main(argv)

    assert not res

    compare_input_output(
        str(scratch_dir),
        str(pathlib.Path(__file__).parent / "data" / "isa_tab" / "expected_output"),
    )
