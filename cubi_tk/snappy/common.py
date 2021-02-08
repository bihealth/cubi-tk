"""Common functionality for SNAPPY."""

import pathlib
import typing

from logzero import logger


#: Dependencies between the SNAPPY steps.
DEPENDENCIES: typing.Dict[str, typing.Tuple[str, ...]] = {
    "ngs_mapping": (),
    "variant_calling": ("ngs_mapping",),
    "variant_export": ("variant_calling",),
    "targeted_seq_cnv_calling": ("ngs_mapping",),
    "targeted_seq_cnv_annotation": ("targeted_seq_cnv_calling",),
    "targeted_seq_cnv_export": ("targeted_seq_cnv_annotation",),
    "wgs_sv_calling": ("ngs_mapping",),
    "wgs_sv_annotation": ("wgs_sv_calling",),
    "wgs_sv_export": ("wgs_sv_annotation",),
    "wgs_cnv_calling": ("ngs_mapping", "variant_calling"),
    "wgs_cnv_annotation": ("wgs_cnv_calling",),
    "wgs_cnv_export": ("wgs_cnv_annotation",),
}


class CouldNotFindPipelineRoot(Exception):
    """Raised when ``.snappy_pipeline`` could not be found."""


def find_snappy_root_dir(
    start_path: typing.Union[str, pathlib.Path], more_markers: typing.Iterable[str] = ()
):
    markers = [".snappy_pipeline"] + list(more_markers)
    start_path = pathlib.Path(start_path)
    for path in [start_path] + list(start_path.parents):
        logger.debug("Trying %s", path)
        if any((path / name).exists() for name in markers):
            logger.info("Will start at %s", path)
            return path
    logger.error("Could not find SNAPPY pipeline directories below %s", start_path)
    raise CouldNotFindPipelineRoot()
