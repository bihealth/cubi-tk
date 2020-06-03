"""Python attrs based models for the SODAR API."""

import typing

import attr


@attr.s(frozen=True, auto_attribs=True)
class OntologyTermRef:
    name: str
    accession: typing.Optional[str]
    ontology_name: typing.Optional[str]


@attr.s(frozen=True, auto_attribs=True)
class Assay:
    file_name: str
    technology_platform: str
    technology_type: OntologyTermRef
    measurement_type: OntologyTermRef
    comments: typing.Dict[str, str]


@attr.s(frozen=True, auto_attribs=True)
class Study:
    identifier: str
    file_name: str
    title: str
    description: str
    comments: typing.Dict[str, str]
    assays: typing.Dict[str, Assay]


@attr.s(frozen=True, auto_attribs=True)
class Investigation:
    archive_name: str
    comments: typing.Any
    description: str
    file_name: str
    identifier: str
    irods_status: bool
    parser_version: str
    project: str
    sodar_uuid: str
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
class LandingZone:
    """Represent a landing zone in the SODAR API."""

    #: UUID of the landing zone.
    sodar_uuid: str
    #: Date of last modification.
    date_modified: str

    #: Status of the landing zone.
    status: str
    #: UUID of the containing project.
    project: str

    #: Title of the landing zone.
    title: str
    #: Description of the landing zone.
    description: str
    #: Owning user.
    user: User

    #: UUID of the related assay.
    assay: str
    #: Status information string.
    status_info: str
    #: Optional configuration name.
    configuration: typing.Optional[typing.Any]
    #: Optional configuration data.
    config_data: typing.Optional[typing.Any]
    #: Path in iRODS.
    irods_path: str
