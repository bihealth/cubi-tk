import uuid
from datetime import datetime

from cubi_tk.sodar import models

import factory


class UserFactory(factory.Factory):
    class Meta:
        model = models.User

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    username = factory.Sequence(lambda n: "user%s" % n)
    name = factory.Sequence(lambda n: "User %d Name" % n)
    email = factory.LazyAttribute(lambda o: "%s@example.org" % o.username)


class AssayFactory(factory.Factory):
    class Meta:
        model = models.Assay

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    file_name = factory.Sequence(lambda n: "a_name_%d" % n)
    irods_path = factory.LazyAttribute(lambda o: "/testZone/path/to/zone/%s" % o.sodar_uuid)
    technology_platform = "PLATFORM"
    technology_type = models.OntologyTermRef("Technology Type", None, None)
    measurement_type = models.OntologyTermRef("Measurement Type", None, None)
    comments = {}


class StudyFactory(factory.Factory):
    class Meta:
        model = models.Study

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    identifier = factory.Sequence(lambda n: "study_%d" % n)
    file_name = factory.Sequence(lambda n: "s_Study_%d" % n)
    irods_path = factory.Sequence(lambda n: "/testZone/path/to/study_%d" % n)
    title = factory.Sequence(lambda n: "Study %d" % n)
    description = factory.Sequence(lambda n: "This is study %d" % n)
    comments = {"Key": "Value"}
    assays = factory.LazyAttribute(lambda o: {a.sodar_uuid: a for a in [AssayFactory()]})


class InvestigationFactory(factory.Factory):
    class Meta:
        model = models.Investigation

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    archive_name = factory.Sequence(lambda n: "Archive_%d.zip" % n)
    comments = []
    description = factory.Sequence(lambda n: "Description %d" % n)
    file_name = factory.Sequence(lambda n: "i_Investigation_%d.txt" % n)
    identifier = factory.Sequence(lambda n: "investigation_%d" % n)
    irods_status = True
    parser_version = factory.Sequence(lambda n: "v0.1.%d" % n)
    project = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    studies = factory.LazyAttribute(lambda n: {s.file_name: s for s in [StudyFactory()]})
    title = factory.Sequence(lambda n: "Investigation %d" % n)


class LandingZoneFactory(factory.Factory):
    class Meta:
        model = models.LandingZone
        exclude = ("_investigation_obj", "_study_obj", "_assay_obj")

    _investigation_obj = factory.SubFactory(InvestigationFactory)
    _study_obj = factory.LazyAttribute(
        lambda o: list(o._investigation_obj.studies.values())[0]  # pylint: disable=protected-access
    )
    _assay_obj = factory.LazyAttribute(
        lambda o: list(o._study_obj.assays.values())[0]  # pylint: disable=protected-access
    )

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    status = "ACTIVE"
    title = factory.Sequence(lambda n: "Landing Zone %d" % n)
    description = factory.Sequence(lambda n: "This is no. %d" % n)
    user = factory.SubFactory(UserFactory)
    date_modified = factory.LazyAttribute(lambda o: datetime.now().isoformat())
    project = factory.LazyAttribute(
        lambda o: o._investigation_obj.project  # pylint: disable=protected-access
    )

    assay = factory.LazyAttribute(
        lambda o: str(o._assay_obj.sodar_uuid)  # pylint: disable=protected-access
    )
    status_info = "Available with write access for user"
    configuration = None
    config_data = None
    irods_path = factory.LazyAttribute(lambda o: "/testZone/path/to/zone/%s" % o.sodar_uuid)
