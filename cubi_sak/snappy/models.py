"""Models used for representing SNAPPY (configuration) data structures."""

import typing

import attr


@attr.s(frozen=True, auto_attribs=True)
class SearchPattern:
    """Represent an entry in the ``search_patterns`` list."""

    #: Pattern for the left-hand side.
    left: str
    #: Optional pattern for the right-hand side.
    right: typing.Optional[str] = None


@attr.s(frozen=True, auto_attribs=True)
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
