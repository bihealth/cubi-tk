"""Python attrs based models for the SODAR API.

In some cases, ``typing.Optional[str]`` is used for the ``sodar_uuid`` attribute as the SODAR API does not return
the UUID as an attribute.
"""

import typing

import attr


@attr.s(frozen=True, auto_attribs=True)
class OntologyTermRef:
    name: str
    accession: typing.Optional[str] = None
    ontology_name: typing.Optional[str] = None


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Assay:
    sodar_uuid: typing.Optional[str] = None
    file_name: str
    irods_path: str
    technology_platform: str
    technology_type: OntologyTermRef
    measurement_type: OntologyTermRef
    comments: typing.Dict[str, str]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Study:
    sodar_uuid: typing.Optional[str] = None
    identifier: str
    file_name: str
    irods_path: str
    title: str
    description: str
    comments: typing.Dict[str, str]
    assays: typing.Dict[str, Assay]


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class Investigation:
    sodar_uuid: typing.Optional[str] = None
    archive_name: str
    comments: typing.Any
    description: str
    file_name: str
    identifier: str
    irods_status: bool
    parser_version: str
    project: str
    studies: typing.Dict[str, Study]
    title: str


@attr.s(frozen=True, auto_attribs=True)
class User:
    """Represents a user in the SODAR API."""

    #: UUID of the user
    sodar_uuid: str
    #: Username of the user
    username: str
    #: Real name of the user
    name: str
    #: Email address of the user
    email: str


@attr.s(frozen=True, auto_attribs=True)
class IrodsDataObject:
    """Represents an iRODS data object in the SODAR API."""

    # File name
    name: str
    # iRODS item type (obj for file)
    type: str
    # Full path to file
    # MAYBE: use a Path object here?
    path: str
    # Size in bytes
    size: int
    # Datetime of last modification (YYYY-MM-DDThh:mm:ssZ)
    # TODO use a datetime object here?
    modify_time: str
    # Checksum of data object (from API version 1.1)
    checksum: str


@attr.s(frozen=True, auto_attribs=True, kw_only=True)
class LandingZone:
    """Represent a landing zone in the SODAR API."""

    #: UUID of the landing zone.
    sodar_uuid: typing.Optional[str] = None
    #: Date of last modification.
    date_modified: str

    #: Status of the landing zone.
    status: str

    status_locked: bool

    #: UUID of the containing project.
    project: str

    #: Title of the landing zone.
    title: str
    #: Description of the landing zone.
    description: str
    #: Owning user.
    user: str

    #: UUID of the related assay.
    assay: str
    #: Status information string.
    status_info: str
    #: Optional configuration name.
    configuration: typing.Optional[typing.Any] = None
    #: Optional configuration data.
    config_data: typing.Optional[typing.Any] = None
    #: Path in iRODS.
    irods_path: str
