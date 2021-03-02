"""Models used for representing SNAPPY (configuration) data structures."""

import pathlib
import typing

import attr
import cattr
from logzero import logger
import yaml


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class SearchPattern:
    """Represent an entry in the ``search_patterns`` list."""

    #: Pattern for the left-hand side.
    left: str
    #: Optional pattern for the right-hand side.
    right: typing.Optional[str] = None


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class DataSet:
    """Represent a data set in the ``config.yaml`` file."""

    #: Path to the file to load the data set from.
    sheet_file: str
    #: The type of the data set.
    sheet_type: str
    #: Search pattern.
    search_patterns: typing.Tuple[SearchPattern, ...]
    #: Tuple of search paths.
    search_paths: typing.Tuple[str, ...]
    #: The naming scheme to use.
    naming_scheme: str = "only_secondary_id"
    #: The optional SODAR UUID.
    sodar_uuid: typing.Optional[str] = None
    #: The optional SODAR title.
    sodar_title: typing.Optional[str] = None


def load_config_yaml(path: pathlib.Path) -> typing.Any:
    with path.open("r") as f:
        try:
            return yaml.safe_load(f)
        except yaml.YAMLError as e:
            logger.error("error: %s", e)


def trans_load(ds):
    """Transmogrify DataSet when loading."""

    def f(k):
        return {"type": "sheet_type", "file": "sheet_file"}.get(k, k)

    return {f(k): v for k, v in ds.items()}


def load_datasets(path: pathlib.Path) -> typing.Dict[str, DataSet]:
    """Load data sets and filter to those with SODAR UUID."""
    logger.info("Loading data sets from %s", path)
    raw_ds = load_config_yaml(path)["data_sets"]
    transmogrified = {key: trans_load(value) for key, value in raw_ds.items()}
    data_sets = cattr.structure(transmogrified, typing.Dict[str, DataSet])
    filtered = {key: ds for key, ds in data_sets.items() if ds.sodar_uuid}
    logger.info("Loaded %d data sets, %d with SODAR UUID", len(data_sets), len(filtered))

    for _key, ds in sorted(filtered.items()):
        logger.debug("  - %s%s", ds.sodar_uuid, ": %s" % ds.sodar_title if ds.sodar_title else "")

    return filtered
