"""Common functionality for SNAPPY."""
import pathlib
import typing

from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from loguru import logger
import yaml


class CouldNotFindPipelineRoot(Exception):
    """Raised when ``.snappy_pipeline`` could not be found."""


class CouldNotFindBioMedSheet(Exception):
    """Raised when BioMedSheet could not be found in configuration file."""


def load_sheet_tsv(path_tsv, tsv_shortcut="germline"):
    """Load sample sheet.

    :param path_tsv: Path to sample sheet TSV file.
    :type path_tsv: pathlib.Path

    :param tsv_shortcut: Sample sheet type. Default: 'germline'.
    :type tsv_shortcut: str

    :return: Returns Sheet model.
    """
    load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % tsv_shortcut)
    with open(path_tsv, "rt") as f:
        return load_tsv(f, naming_scheme=NAMING_ONLY_SECONDARY_ID)


def find_snappy_root_dir(
    start_path: typing.Union[str, pathlib.Path], more_markers: typing.Iterable[str] = ()
):
    """Find snappy pipeline root directory.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :param more_markers: Additional markers to be included in the search. Method will always use '.snappy_pipeline'.
    :type more_markers: Iterable

    :return: Returns path to snappy pipeline root directory.

    :raises CouldNotFindPipelineRoot: if cannot find pipeline root.
    """
    markers = [".snappy_pipeline"] + list(more_markers)
    start_path = pathlib.Path(start_path)
    for path in [start_path] + list(start_path.parents):
        logger.debug("Trying {}", path)
        if any((path / name).exists() for name in markers):
            logger.info("Will start at {}", path)
            return path
    logger.error("Could not find SNAPPY pipeline directories below {}", start_path)
    raise CouldNotFindPipelineRoot()


def get_biomedsheet_path(start_path, uuid):
    """Get biomedsheet path, i.e., sample sheet.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :param uuid: Project UUID.
    :type uuid: str

    :return: Returns path to sample sheet.
    """
    # Initialise variables
    biomedsheet_path = None

    # Find config file
    snappy_dir_parent = find_snappy_root_dir(start_path=start_path)
    snappy_config = snappy_dir_parent / ".snappy_pipeline" / "config.yaml"

    # Load config
    with open(snappy_config, "r") as stream:
        config = yaml.safe_load(stream)

    # Search config for the correct dataset
    for project in config["data_sets"]:
        dataset = config["data_sets"].get(project)
        try:
            if dataset["sodar_uuid"] == uuid:
                biomedsheet_path = snappy_dir_parent / ".snappy_pipeline" / dataset["file"]
        except KeyError:
            # Not every dataset has an associated UUID
            logger.info("Data set '{0}' has no associated UUID.".format(project))

    # Raise exception if none found
    if biomedsheet_path is None:
        tpl = "Could not find sample sheet for UUID {uuid}. Dataset configuration: {config}"
        config_str = "; ".join(["{} = {}".format(k, v) for k, v in config["data_sets"].items()])
        msg = tpl.format(uuid=uuid, config=config_str)
        raise CouldNotFindBioMedSheet(msg)

    # Return path
    return biomedsheet_path


def get_all_biomedsheet_paths(start_path):
    """Get paths to all biomedsheet files in a SNAPPY directory.

    :param start_path: Start path to search for snappy root directory.
    :type start_path: str, pathlib.Path

    :return: Returns paths to sample sheet.
    """
    result = []

    # Find config file
    snappy_dir_parent = find_snappy_root_dir(start_path=start_path)
    snappy_config = snappy_dir_parent / ".snappy_pipeline" / "config.yaml"

    # Load config
    with open(snappy_config, "r") as stream:
        config = yaml.safe_load(stream)

    # Search config for the datasets.
    for project in config["data_sets"]:
        dataset = config["data_sets"].get(project)
        result.append(snappy_dir_parent / ".snappy_pipeline" / dataset["file"])

    return result
