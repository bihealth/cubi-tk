"""``cubi-tk isa-tab add-ped``: add records to ISA-tab from PED file."""

import argparse
import pathlib
import itertools
import io
import typing

from altamisa.isatab import (
    InvestigationWriter,
    AssayWriter,
    StudyWriter,
    Study,
    Assay,
    Arc,
    Material,
    Process,
    Characteristics,
    FactorValue,
    Comment,
    ParameterValue,
    OntologyTermRef,
)
from altamisa.constants.table_headers import (
    CHARACTERISTICS,
    COMMENT,
    FACTOR_VALUE,
    PARAMETER_VALUE,
    SOURCE_NAME,
    SAMPLE_NAME,
    MATERIAL_NAME_HEADERS,
    PROCESS_NAME_HEADERS,
    DATE,
    LABEL,
    MATERIAL_TYPE,
    PERFORMER,
    EXTRACT_NAME,
    LIBRARY_NAME,
    UNIT,
    RAW_DATA_FILE,
)
import attr
from logzero import logger

from .. import parse_ped
from .. import isa_support
from ..common import overwrite_helper


@attr.s(frozen=True, auto_attribs=True)
class Config:
    verbose: bool
    config: str
    sodar_server_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    no_warnings: bool
    sample_name_normalization: str
    yes: bool
    dry_run: bool
    show_diff: bool
    show_diff_side_by_side: bool
    batch_no: str
    library_layout: str
    library_type: str
    library_kit: str
    library_kit_catalogue_id: str
    platform: str
    instrument_model: str
    input_investigation_file: str
    input_ped_file: str


def normalize_snappy(s):
    """Normalization function that performs SNAPPY normalization (hyphen to underscore)."""
    return s.replace("-", "_")


def normalize_none(s):
    """Normalization function that performs no normalization."""
    return s


#: Normalize sample name function.
NORMALIZE = {"snappy": normalize_snappy, "none": normalize_none}

#: Mapping for sex.
SEX = {"1": "1", "2": "2", "M": "1", "F": "2"}

#: Mapping for disease.
DISEASE = {"1": "1", "2": "2", "N": "1", "Y": "2"}


#: Mapping from column type to value class.
COLUMN_TO_CLASS = {
    CHARACTERISTICS: Characteristics,
    FACTOR_VALUE: FactorValue,
    COMMENT: Comment,
    PARAMETER_VALUE: ParameterValue,
    # Simple / standard annotations
    PERFORMER: None,
    DATE: None,
    LABEL: None,
    UNIT: None,
    MATERIAL_TYPE: None,
}

#: Mapping from column type to attribute
COLUMN_TO_ATTR_NAME = {
    CHARACTERISTICS: "characteristics",
    FACTOR_VALUE: "factor_values",
    COMMENT: "comments",
    PARAMETER_VALUE: "parameter_values",
    PERFORMER: "performer",
    DATE: "date",
    LABEL: "label",
    UNIT: "unit",
    MATERIAL_TYPE: "material_type",
}


class SheetUpdateVisitor(isa_support.IsaNodeVisitor):
    """IsaNodeVisitor that updates the ISA sample sheet as we walk along it."""

    def __init__(self, donor_map):
        #: Mapping from normalized donor name to Donor instance.
        self.donor_map = donor_map
        #: The source names seen so far.
        self.seen_source_names = set()

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        if material.type.lower() == "source name":
            self.seen_source_names.add(material.name)
            if material.name in self.donor_map:
                donor = self.donor_map[material.name]
                characteristics = []
                for c in material.characteristics:
                    if c.name.lower() == "father":
                        c = attr.evolve(c, value=[donor.father_name])
                    elif c.name.lower() == "mother":
                        c = attr.evolve(c, value=[donor.mother_name])
                    characteristics.append(c)
                return attr.evolve(material, characteristics=tuple(characteristics))


def _append_study_line(study, donor, materials, processes, arcs, config):
    """Create extra materials/processes/arcs for extra line in study table."""
    counter = 0  # used for creating unique names
    prev = None  # previous node
    curr = None  # current node, determines type
    attr_name = None
    prev_label = ""
    for col in study.header:
        klass = COLUMN_TO_CLASS[col.column_type]
        attr_name = COLUMN_TO_ATTR_NAME[col.column_type]

        if col.column_type in MATERIAL_NAME_HEADERS:
            # New material.
            prev = curr
            if col.column_type in (SOURCE_NAME, SAMPLE_NAME):
                curr = {
                    "type": col.column_type,
                    "unique_name": "study-%s-%s-%d" % (col.column_type, donor.name, counter),
                    "name": donor.name if col.column_type == SOURCE_NAME else "%s-N1" % donor.name,
                    "extract_label": None,
                    "characteristics": [],
                    "comments": [],
                    "factor_values": [],
                    "material_type": None,
                    "headers": col.get_simple_string(),
                }
            else:
                raise Exception("Invalid material type: %s" % col.column_type)
            counter += 1
            materials.append(curr)
            if prev:
                arcs.append(Arc(prev["unique_name"], curr["unique_name"]))
        elif col.column_type == "Protocol REF" or col.column_type in PROCESS_NAME_HEADERS:
            # New protocol.
            prev = curr
            curr = {
                "protocol_ref": "Sample collection",
                "unique_name": "study-%s-%s-%d" % (col.column_type, donor.name, counter),
                "name": None,
                "name_type": None,
                "date": None,
                "performer": "",
                "parameter_values": [],
                "array_design_ref": None,
                "first_dimension": None,
                "second_dimension": None,
                "comments": [],
                "headers": col.get_simple_string(),
            }
            counter += 1
            processes.append(curr)
            if prev:
                arcs.append(Arc(prev["unique_name"], curr["unique_name"]))
        else:
            handled_term_ref_attrs = ("organism",)
            curr["headers"] += col.get_simple_string()
            if col.column_type == "Term Source REF":
                if prev_label not in handled_term_ref_attrs:
                    old = curr[attr_name][-1]
                    curr[attr_name][-1] = attr.evolve(
                        old,
                        value=[
                            OntologyTermRef(name=v, accession="", ontology_name="")
                            for v in old.value
                        ],
                    )
                continue

            value = ""
            if hasattr(col, "label"):
                if col.label.lower() == "external links" and curr["type"] == SOURCE_NAME:
                    # TODO: hacky, would need original donor ID here
                    value = "x-charite-medgen-blood-book-id:%s" % donor.name.replace("_", "-")
                elif col.label.lower() == "batch":
                    value = str(config.batch_no)
                elif col.label.lower() == "family":
                    value = donor.family_id
                elif col.label.lower() == "organism":
                    value = OntologyTermRef(
                        name="Homo sapiens",
                        accession="http://purl.bioontology.org/ontology/NCBITAXON/9606",
                        ontology_name="NCBITAXON",
                    )
                elif col.label.lower() == "father":
                    value = donor.father_name
                elif col.label.lower() == "mother":
                    value = donor.mother_name
                elif col.label.lower() == "sex":
                    value = donor.sex
                elif col.label.lower() == "disease status":
                    value = donor.disease

            if col.column_type in (DATE, LABEL, MATERIAL_TYPE, PERFORMER):
                pass  # do nothing
            elif col.column_type == "Comment":
                curr[attr_name].append(klass(name=col.label, value=[value]))
                prev_label = col.label.lower()
            else:
                curr[attr_name].append(klass(name=col.label, value=[value], unit=None))
                prev_label = col.label.lower()


def _append_assay_line(assay, donor, materials, processes, arcs, config):
    """Create extra materials/processes/arcs for extra line in assay table."""
    counter = 0  # used for creating unique names
    prev = None  # previous node
    curr = None  # current node, determines type
    attr_name = None
    prev_label = ""
    seen_extract_name = False
    protocol_refs = 0
    prev_attr_name = None
    prev_unit_container = None
    for col in assay.header:
        klass = COLUMN_TO_CLASS[col.column_type]
        attr_name = COLUMN_TO_ATTR_NAME[col.column_type]

        if col.column_type in MATERIAL_NAME_HEADERS:
            prev = curr
            if col.column_type == SAMPLE_NAME:
                name = "%s-N1" % donor.name
            elif col.column_type == EXTRACT_NAME:
                if seen_extract_name:
                    raise Exception("Seen column Extract Name twice!")
                else:
                    name = "%s-N1-DNA1" % donor.name
                    seen_extract_name = True
            elif col.column_type == LIBRARY_NAME:
                name = "%s-N1-DNA1-%s" % (donor.name, config.library_type)
            elif col.column_type == LIBRARY_NAME:
                name = "%s-N1-DNA1-%s" % (donor.name, config.library_type)
            elif col.column_type == RAW_DATA_FILE:
                name = ""
            else:
                raise Exception("Unexpected material type %s" % col.column_type)
            curr = {
                "type": col.column_type,
                "unique_name": "assay-%s-%s-%d" % (col.column_type, donor.name, counter),
                "name": name,
                "extract_label": None,
                "characteristics": [],
                "comments": [],
                "factor_values": [],
                "material_type": None,
                "headers": col.get_simple_string(),
            }
            counter += 1
            materials.append(curr)
            if prev:
                arcs.append(Arc(prev["unique_name"], curr["unique_name"]))
        elif col.column_type == "Protocol REF" or col.column_type in PROCESS_NAME_HEADERS:
            if protocol_refs == 0:
                protocol_ref = "Nucleic acid extraction %s" % config.library_type
            elif protocol_refs == 1:
                protocol_ref = "Library construction %s" % config.library_type
            elif protocol_refs == 2:
                protocol_ref = "Nucleic acid sequencing %s" % config.library_type
            else:
                raise Exception("Seen too many Protocol REF headers!")
            protocol_refs += 1

            prev = curr
            curr = {
                "protocol_ref": protocol_ref,
                "unique_name": "assay-%s-%s-%d" % (col.column_type, donor.name, counter),
                "name": None,
                "name_type": None,
                "date": "",
                "performer": "",
                "parameter_values": [],
                "array_design_ref": None,
                "first_dimension": None,
                "second_dimension": None,
                "comments": [],
                "headers": col.get_simple_string(),
            }
            processes.append(curr)
            if prev:
                arcs.append(Arc(prev["unique_name"], curr["unique_name"]))
            counter += 1
        else:
            handled_term_ref_attrs = ()
            curr["headers"] += col.get_simple_string()
            if col.column_type == "Term Source REF":
                if prev_label not in handled_term_ref_attrs:
                    if prev_unit_container:
                        new_container = attr.evolve(
                            prev_unit_container,
                            unit=OntologyTermRef(
                                name=prev_unit_container.unit, accession="", ontology_name=""
                            ),
                        )
                        curr[prev_attr_name][-1] = new_container
                        prev_unit_container = None
                    else:
                        old = curr[attr_name][-1]
                        curr[attr_name][-1] = attr.evolve(
                            old,
                            value=[
                                OntologyTermRef(name=v, accession="", ontology_name="")
                                for v in old.value
                            ],
                        )
                continue
            klass = COLUMN_TO_CLASS[col.column_type]
            attr_name = COLUMN_TO_ATTR_NAME[col.column_type]

            value = ""
            if hasattr(col, "label"):
                if col.label.lower() == "library source":
                    value = "GENOMIC"
                elif col.label.lower() == "library strategy":
                    if config.library_type == "WES":
                        value = "WXS"
                    elif config.library_type == "WGS":
                        value = "WGS"
                    elif config.library_type == "Panel_seq":
                        value = "PANEL"
                    else:
                        raise Exception("Invalid library strategy")
                elif col.label.lower() == "library selection":
                    if config.library_type == "WES":
                        value = "Hybrid Selection"
                    elif config.library_type == "WGS":
                        value = "RANDOM"
                    elif config.library_type == "Panel_seq":
                        value = "Hybrid Selection"
                    else:
                        raise Exception("Invalid library selection")
                elif col.label.lower() == "library layout":
                    value = "PAIRED"
                elif col.label.lower() == "library kit":
                    value = config.library_kit
                elif col.label.lower() == "library kit catalogue id":
                    value = config.library_kit_catalogue_id
                elif col.label.lower() == "folder name":
                    # TODO: hacky, actually need real donor ID
                    value = donor.name.replace("_", "-")
                elif col.label.lower() == "platform":
                    value = config.platform
                elif col.label.lower() == "instrument model":
                    value = config.instrument_model
                elif col.label.lower() == "base quality encoding":
                    value = "Phred+33"

            if col.column_type in (DATE, LABEL, MATERIAL_TYPE, PERFORMER):
                pass  # do nothing
            elif col.column_type == UNIT:
                curr[prev_attr_name][-1] = attr.evolve(curr[prev_attr_name][-1], unit=value)
                prev_unit_container = curr[prev_attr_name][-1]
            elif col.column_type == COMMENT:
                curr[attr_name].append(klass(name=col.label, value=[value]))
                prev_label = col.label.lower()
                prev_attr_name = attr_name
            else:
                curr[attr_name].append(klass(name=col.label, value=[value], unit=None))
                prev_label = col.label.lower()
                prev_attr_name = attr_name


def isa_germline_append_donors(
    studies: typing.Dict[str, Study],
    assays: typing.Dict[str, Assay],
    ped_donors: typing.Tuple[parse_ped.Donor, ...],
    config: Config,
) -> typing.Tuple[typing.Dict[str, Study], typing.Dict[str, Assay]]:
    assert len(studies) == 1, "Only one study supported at the moment"
    assert len(assays) == 1, "Only one assay supported at the moment"

    # Add additional lines to the study.
    study = list(studies.values())[0]
    sms: typing.List[typing.Dict[str, typing.Any]] = []
    sps: typing.List[typing.Dict[str, typing.Any]] = []
    sas: typing.List[Arc] = []
    for donor in ped_donors:
        _append_study_line(study, donor, sms, sps, sas, config)
    study = attr.evolve(
        study,
        materials={**study.materials, **{x["unique_name"]: Material(**x) for x in sms}},
        processes={**study.processes, **{x["unique_name"]: Process(**x) for x in sps}},
        arcs=tuple(itertools.chain(study.arcs, sas)),
    )

    # Add additional lines to the assay.
    assay = list(assays.values())[0]
    ams: typing.List[typing.Dict[str, typing.Any]] = []
    aps: typing.List[typing.Dict[str, typing.Any]] = []
    aas: typing.List[Arc] = []
    for donor in ped_donors:
        _append_assay_line(assay, donor, ams, aps, aas, config)
    assay = attr.evolve(
        assay,
        materials={**assay.materials, **{x["unique_name"]: Material(**x) for x in ams}},
        processes={**assay.processes, **{x["unique_name"]: Process(**x) for x in aps}},
        arcs=tuple(itertools.chain(assay.arcs, aas)),
    )

    # Return the updated assay.
    return {list(studies.keys())[0]: study}, {list(assays.keys())[0]: assay}


class AddPedIsaTabCommand:
    """Implementation of the ``add-ped`` command."""

    def __init__(self, config: Config):
        #: Command line arguments.
        self.config = config

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="isa_tab_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--sample-name-normalization",
            default="snappy",
            choices=("snappy", "none"),
            help="Normalize sample names, default: snappy, choices: snappy, none",
        )

        parser.add_argument(
            "--yes", default=False, action="store_true", help="Assume all answers are yes."
        )

        parser.add_argument(
            "--dry-run",
            "-n",
            default=False,
            action="store_true",
            help="Perform a dry run, i.e., don't change anything only display change, implies '--show-diff'.",
        )
        parser.add_argument(
            "--no-show-diff",
            "-D",
            dest="show_diff",
            default=True,
            action="store_false",
            help="Don't show change when creating/updating sample sheets.",
        )
        parser.add_argument(
            "--show-diff-side-by-side",
            default=False,
            action="store_true",
            help="Show diff side by side instead of unified.",
        )

        parser.add_argument("--batch-no", default=".", help="Value to set as the batch number.")
        parser.add_argument(
            "--library-type",
            default="WES",
            choices=("WES", "WGS", "Panel_seq"),
            help="The library type.",
        )
        parser.add_argument(
            "--library-layout",
            default="PAIRED",
            choices=("SINGLE", "PAIRED"),
            help="The library layout.",
        )
        parser.add_argument("--library-kit", default="", help="The library kit used.")
        parser.add_argument(
            "--library-kit-catalogue-id", default="", help="The library kit catalogue ID."
        )
        parser.add_argument(
            "--platform", default="ILLUMINA", help="The string to use for the platform"
        )
        parser.add_argument(
            "--instrument-model", default="", help="The string to use for the instrument model"
        )

        parser.set_defaults(no_warnings=False)
        parser.add_argument(
            "input_investigation_file",
            metavar="investigation.tsv",
            help="Path to ISA-tab investigation file.",
        )
        parser.add_argument(
            "input_ped_file",
            metavar="pedigree.ped",
            help="Path to PLINK PED file with records to add.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        args = vars(args)
        args.pop("cmd", None)
        args.pop("isa_tab_cmd", None)
        return cls(Config(**args)).execute()

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        logger.info("Starting cubi-tk isa-tab add-ped")
        logger.info("  config: %s", self.config)

        isa_data = isa_support.load_investigation(self.config.input_investigation_file)
        if len(isa_data.studies) > 1 or len(isa_data.assays) > 1:
            logger.error("Only one study and assay per ISA-tab supported at the moment.")
            return 1
        ped_donors = list(parse_ped.parse_ped(self.config.input_ped_file))
        if not ped_donors:
            logger.error("No donor in pedigree")
            return 1

        self._perform_update(isa_data, ped_donors)
        return 0

    def _perform_update(self, isa, ped_donors):
        # Traverse investigation, studies, assays, potentially updating the nodes.
        donor_map = self._build_donor_map(ped_donors)
        visitor = SheetUpdateVisitor(donor_map)
        iwalker = isa_support.InvestigationTraversal(isa.investigation, isa.studies, isa.assays)
        iwalker.run(visitor)
        investigation, studies, assays = iwalker.build_evolved()

        # Add records to study and assay for donors not seen so far.
        todo_ped_donors = [
            donor for donor in donor_map.values() if donor.name not in visitor.seen_source_names
        ]
        studies, assays = isa_germline_append_donors(
            studies, assays, tuple(todo_ped_donors), self.config
        )
        new_isa = attr.evolve(isa, investigation=investigation, studies=studies, assays=assays)

        # Write ISA-tab into string buffers.
        io_investigation = io.StringIO()
        InvestigationWriter.from_stream(isa.investigation, io_investigation).write()
        ios_studies = {}
        for name, study in new_isa.studies.items():
            ios_studies[name] = io.StringIO()
            StudyWriter.from_stream(study, ios_studies[name]).write()
        ios_assays = {}
        for name, assay in new_isa.assays.items():
            ios_assays[name] = io.StringIO()
            AssayWriter.from_stream(assay, ios_assays[name]).write()

        # Write out updated ISA-tab files using the diff helper.
        i_path = pathlib.Path(self.config.input_investigation_file)
        overwrite_helper(
            i_path,
            io_investigation.getvalue(),
            do_write=not self.config.dry_run,
            show_diff=True,
            show_diff_side_by_side=self.config.show_diff_side_by_side,
            answer_yes=self.config.yes,
        )
        for filename, ios_study in ios_studies.items():
            overwrite_helper(
                i_path.parent / filename,
                ios_study.getvalue(),
                do_write=not self.config.dry_run,
                show_diff=True,
                show_diff_side_by_side=self.config.show_diff_side_by_side,
                answer_yes=self.config.yes,
            )
        for filename, ios_assay in ios_assays.items():
            overwrite_helper(
                i_path.parent / filename,
                ios_assay.getvalue(),
                do_write=not self.config.dry_run,
                show_diff=True,
                show_diff_side_by_side=self.config.show_diff_side_by_side,
                answer_yes=self.config.yes,
            )

    def _build_donor_map(self, ped_donors):
        # Find name of index for each family.
        indexes = {}  # by family
        # First, case family == index name
        for ped_donor in ped_donors:
            if ped_donor.name == ped_donor.family_id:
                indexes[ped_donor.family_id] = ped_donor
        # Second case, first affected in each family.
        for ped_donor in ped_donors:
            if ped_donor.disease in ("2", "Y") and ped_donor.family_id not in indexes:
                indexes[ped_donor.family_id] = ped_donor
        # Third case, first in each family.
        for ped_donor in ped_donors:
            if ped_donor.family_id not in indexes:
                indexes[ped_donor.family_id] = ped_donor

        # Build donor from normalized name do Donor.
        normalize = NORMALIZE[self.config.sample_name_normalization]
        return {
            normalize(donor.name): parse_ped.Donor(
                family_id="FAM_%s" % normalize(indexes[donor.family_id].name),
                name=normalize(donor.name),
                father_name=normalize(donor.father_name),
                mother_name=normalize(donor.mother_name),
                sex=donor.sex,
                disease=donor.disease,
            )
            for donor in ped_donors
        }


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk isa-tab add-ped``."""
    return AddPedIsaTabCommand.setup_argparse(parser)
