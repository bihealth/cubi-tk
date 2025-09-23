from datetime import datetime
import uuid

import factory

from cubi_tk import api_models


def return_api_investigation_mock():
    investigation = api_models.Investigation(
        sodar_uuid="c339b4de-23a9-4cc3-8801-5f65b4739680",
        archive_name="None",
        comments={
            "Created With Configuration": "/path/to/isa-configurations/bih_studies/bih_cancer",
            "Last Opened With Configuration": "bih_cancer",
        },
        description="",
        file_name="i_Investigation.txt",
        identifier="",
        irods_status=True,
        parser_version="0.2.9",
        project="ad002ac2-b06c-4012-9dc4-8c2ade3e7378",
        studies={
            "7b5f6a28-92d0-4871-8cba-8c74db8ee298": api_models.Study(
                sodar_uuid="7b5f6a28-92d0-4871-8cba-8c74db8ee298",
                identifier="investigation_title",
                file_name="s_investigation_title.txt",
                irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298",
                title="Investigation Title",
                description="",
                comments={"Study Grant Number": "", "Study Funding Agency": ""},
                assays={
                    "992dc872-0033-4c3b-817b-74b324327e7d": api_models.Assay(
                        sodar_uuid="992dc872-0033-4c3b-817b-74b324327e7d",
                        file_name="a_investigation_title_exome_sequencing_second.txt",
                        irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298/assay_992dc872-0033-4c3b-817b-74b324327e7d",
                        technology_platform="Illumina",
                        technology_type=api_models.OntologyTermRef(
                            name="nucleotide sequencing",
                            accession="http://purl.obolibrary.org/obo/OBI_0000626",
                            ontology_name="OBI",
                        ),
                        measurement_type=api_models.OntologyTermRef(
                            name="exome sequencing", accession=None, ontology_name=None
                        ),
                        comments={},
                    ),
                    "bd3e98a0-e2a9-48ad-b2bc-d10d407307f2": api_models.Assay(
                        sodar_uuid="bd3e98a0-e2a9-48ad-b2bc-d10d407307f2",
                        file_name="a_investigation_title_exome_sequencing.txt",
                        irods_path="/sodarZone/projects/ad/ad002ac2-b06c-4012-9dc4-8c2ade3e7378/sample_data/study_7b5f6a28-92d0-4871-8cba-8c74db8ee298/assay_bd3e98a0-e2a9-48ad-b2bc-d10d407307f2",
                        technology_platform="Illumina",
                        technology_type=api_models.OntologyTermRef(
                            name="nucleotide sequencing",
                            accession="http://purl.obolibrary.org/obo/OBI_0000626",
                            ontology_name="OBI",
                        ),
                        measurement_type=api_models.OntologyTermRef(
                            name="exome sequencing", accession=None, ontology_name=None
                        ),
                        comments={},
                    ),
                },
            )
        },
        title="Investigation Title",
    )
    return investigation


class UserFactory(factory.Factory):
    class Meta:
        model = api_models.User

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    username = factory.Sequence(lambda n: "user%s" % n)
    name = factory.Sequence(lambda n: "User %d Name" % n)
    email = factory.LazyAttribute(lambda o: "%s@example.org" % o.username)


class AssayFactory(factory.Factory):
    class Meta:
        model = api_models.Assay

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    file_name = factory.Sequence(lambda n: "a_name_%d" % n)
    irods_path = factory.LazyAttribute(lambda o: "/testZone/path/to/zone/%s" % o.sodar_uuid)
    technology_platform = "PLATFORM"
    technology_type = api_models.OntologyTermRef("Technology Type", None, None)
    measurement_type = api_models.OntologyTermRef("Measurement Type", None, None)
    comments = {}


class StudyFactory(factory.Factory):
    class Meta:
        model = api_models.Study

    sodar_uuid = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
    identifier = factory.Sequence(lambda n: "study_%d" % n)
    file_name = factory.Sequence(lambda n: "s_Study_%d.txt" % n)
    irods_path = factory.Sequence(lambda n: "/testZone/path/to/study_%d" % n)
    title = factory.Sequence(lambda n: "Study %d" % n)
    description = factory.Sequence(lambda n: "This is study %d" % n)
    comments = {"Key": "Value"}
    assays = factory.LazyAttribute(lambda o: {a.sodar_uuid: a for a in [AssayFactory()]})


class InvestigationFactory(factory.Factory):
    class Meta:
        model = api_models.Investigation

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
        model = api_models.LandingZone
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
    user = factory.LazyAttribute(lambda o: str(uuid.uuid4()))
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
