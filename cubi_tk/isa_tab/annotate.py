"""``cubi-tk isa-tab annotate``: add annotation to ISA-tab from CSV file."""

import argparse
import csv
import io
import pathlib
import typing
from warnings import warn

import attr
from altamisa.constants.table_headers import (
    SAMPLE_NAME,
    MATERIAL_NAME_HEADERS,
    PROCESS_NAME_HEADERS,
    PROTOCOL_REF,
    DATA_FILE_HEADERS,
)
from altamisa.isatab import InvestigationWriter, AssayWriter, StudyWriter, Characteristics, Comment
from altamisa.isatab.helpers import is_ontology_term_ref
from logzero import logger

from .. import isa_support
from ..common import overwrite_helper


@attr.s(frozen=False, auto_attribs=True)
class Config:
    verbose: bool
    config: str
    force_update: bool
    sodar_server_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    no_warnings: bool
    yes: bool
    dry_run: bool
    show_diff: bool
    show_diff_side_by_side: bool
    input_investigation_file: str
    input_annotation_file: str
    target_study: str
    target_assay: str


class SheetUpdateVisitor(isa_support.IsaNodeVisitor):
    """IsaNodeVisitor that updates the ISA sample sheet as we walk along it."""

    def __init__(self, annotation_map, header_map, overwrite, target_study, target_assay):
        #: Mapping from normalized donor name to Donor instance.
        self.annotation_map = annotation_map
        self.header_map = header_map
        self.overwrite = overwrite
        #: The source names seen so far.
        self.seen_source_names = set()
        #: Study and assay selected for annotation update
        self.target_study = target_study
        self.target_assay = target_assay

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)

        # Update material node only if part of targeted study or assay
        if study.file.name.endswith(self.target_study) and not (
            assay and not assay.file.name.endswith(self.target_assay)
        ):
            if material.type in DATA_FILE_HEADERS:
                return self._update_comments(material)
            else:  # Normal Materials
                return self._update_characteristics(material, assay)

        return None

    def _has_content(self, value):
        if is_ontology_term_ref(value):
            return value.name or value.accession or value.ontology_name
        else:
            return value

    def _update_characteristics(self, material, assay):
        if assay and material.type == SAMPLE_NAME:
            return material
        elif (
            material.type in self.annotation_map
            and material.name in self.annotation_map[material.type]
        ):
            annotation = self.annotation_map[material.type][material.name]
            characteristics = []
            # update available characteristics
            for c in material.characteristics:
                if c.name in annotation:
                    if self._has_content(c.value[0]):
                        if self.overwrite:
                            c = attr.evolve(c, value=[annotation.pop(c.name)])
                            # TODO: consider ontologies as values
                        else:
                            warn(
                                f"Value for material {material.name} and characteristic {c.name} "
                                "already exist and --force-update not set. Skipping..."
                            )
                            annotation.pop(c.name)
                    else:
                        c = attr.evolve(c, value=[annotation.pop(c.name)])
                characteristics.append(c)
            # add new characteristics
            if annotation:
                for col_name, annotation_value in annotation.items():
                    c = Characteristics(name=col_name, value=[annotation_value], unit=None)
                    # TODO: consider ontologies, units
                    characteristics.append(c)
                    material = attr.evolve(
                        material, headers=material.headers + [f"Characteristics[{col_name}]"]
                    )
            return attr.evolve(material, characteristics=tuple(characteristics))
        elif material.type in self.header_map and not all(
            x in material.headers for x in self.header_map[material.type].values()
        ):
            # header update
            material_headers = material.headers
            material_characteristics = list(material.characteristics)
            for char_name, isatab_col_name in self.header_map[material.type].items():
                if isatab_col_name not in material.headers:
                    material_headers.append(isatab_col_name)
                    material_characteristics.append(
                        Characteristics(name=char_name, value=[""], unit=None)
                    )
            return attr.evolve(
                material, headers=material_headers, characteristics=material_characteristics
            )
        else:
            return material

    def _update_comments(self, material):
        if (
            material.type in self.annotation_map
            and material.name in self.annotation_map[material.type]
        ):
            annotation = self.annotation_map[material.type][material.name]
            comments = []
            # update available comments
            for c in material.comments:
                if c.name in annotation:
                    if c.value:
                        if self.overwrite:
                            c = attr.evolve(c, value=[annotation.pop(c.name)])
                        else:
                            warn(
                                f"Value for material {material.name} and comment {c.name} "
                                "already exist and --force-update not set. Skipping..."
                            )
                            annotation.pop(c.name)
                    else:
                        c = attr.evolve(c, value=[annotation.pop(c.name)])
                comments.append(c)
            # add new comments
            if annotation:
                for col_name, annotation_value in annotation.items():
                    c = Comment(name=col_name, value=annotation_value)
                    comments.append(c)
                    material = attr.evolve(
                        material, headers=material.headers + [f"Comment[{col_name}]"]
                    )
            return attr.evolve(material, comments=tuple(comments))
        elif material.type in self.header_map and not all(
            x in material.headers for x in self.header_map[material.type].values()
        ):
            # header update
            material_headers = material.headers
            material_comments = list(material.comments)
            for comment_name, isatab_col_name in self.header_map[material.type].items():
                if isatab_col_name not in material.headers:
                    material_headers.append(isatab_col_name)
                    material_comments.append(Comment(name=comment_name, value=""))
            return attr.evolve(material, headers=material_headers, comments=material_comments)
        else:
            return material


class AddAnnotationIsaTabCommand:
    """Implementation of the ``annotate`` command."""

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
        parser.add_argument(
            "--force-update",
            default=False,
            action="store_true",
            help="Overwrite non-empty ISA-tab entries.",
        )

        # parser.add_argument("--batch-no", default=".", help="Value to set as the batch number.")

        parser.set_defaults(no_warnings=False)
        parser.add_argument(
            "input_investigation_file",
            metavar="investigation.tsv",
            help="Path to ISA-tab investigation file.",
        )
        parser.add_argument(
            "input_annotation_file",
            metavar="annotation.tsv",
            help="Path to annotation (TSV) file with information to add.",
        )

        parser.add_argument(
            "--target-study",
            "-s",
            metavar="s_study.tsv",
            help="File name study to annotate. If not provided, first study in investigation is used.",
        )
        parser.add_argument(
            "--target-assay",
            "-a",
            metavar="a_assay.tsv",
            help="File name of assay to annotate. If not provided, first assay in investigation is used.",
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
        """Execute the annotation."""
        logger.info("Starting cubi-tk isa-tab annotate")
        logger.info("  config: %s", self.config)

        # Read isa-tab file
        isa_data = isa_support.load_investigation(self.config.input_investigation_file)

        # Check target study/assay availability
        if not self._check_studies_and_assays(isa_data):
            return 1

        # Read annotation file and build mapping
        annotation_data = self._read_annotation(self.config.input_annotation_file)
        annotation_map, header_map = self._build_annotation_map(annotation_data)

        # Add annotation
        self._perform_update(isa_data, annotation_map, header_map)

        return 0

    def _check_studies_and_assays(self, isa_data):
        # List studies
        study_file_names = list(isa_data.studies.keys())
        # Check that a least one study exists
        if len(study_file_names) > 0:
            # If no target study declared, use first study
            if not self.config.target_study:
                self.config.target_study = study_file_names[0]
            # Check if target study is in list (i.e. in investigation)
            elif self.config.target_study not in study_file_names:
                logger.error(
                    "Invalid target study '%s'.\nStudies available:\n%s",
                    self.config.target_study,
                    study_file_names,
                )
                return False
        else:
            logger.error("No studies available in investigation.")
            return False

        # List assays
        assay_file_names = list(isa_data.assays.keys())
        # Check that a least one assay exists
        if len(assay_file_names) > 0:
            # If no target assay declared, use first assay
            if not self.config.target_assay:
                self.config.target_assay = assay_file_names[0]
            # Check if target assay is in list (i.e. in investigation)
            if self.config.target_assay not in assay_file_names:
                logger.error(
                    "Invalid target assay '%s'.\nAssays available:\n%s",
                    self.config.target_assay,
                    assay_file_names,
                )
                return False
        else:
            logger.error("No assays available in investigation.")
            return False

        return True

    def _read_annotation(self, filename):
        with open(filename) as infile:
            anno_reader = csv.reader(infile, delimiter="\t")
            annotation_data = []
            for row in anno_reader:
                annotation_data.append(row)

        # Need at least two columns (e.g. one source/sample name plus new annotation) and one data row
        if len(annotation_data) < 2 and len(annotation_data[0]) < 2:
            logger.error("Annotation file needs at least two columns and two rows")
            return 1
        return annotation_data

    def _build_annotation_map(self, annotation):
        # change to long df
        # node_type: material type, e.g. Source Name, Sample Name, etc.
        # ID: actual source name, sample name, etc.
        # col_name: annotation key, e.g. name to use for characteristic, comment, etc.
        # annotation_value: value to assign
        long_df = {"node_type": [], "ID": [], "col_name": [], "annotation_value": []}
        node_type = None
        header = annotation.pop(0)
        if header[0] not in MATERIAL_NAME_HEADERS:
            raise ValueError(
                f"Error in annotation file: first column header must be one of: {', '.join(MATERIAL_NAME_HEADERS)}."
            )
        for i, col in enumerate(header):
            if col in list(PROCESS_NAME_HEADERS) + [PROTOCOL_REF]:
                raise ValueError(
                    "Error in annotation file: Process parameter annotation currently not supported."
                )
            if col in MATERIAL_NAME_HEADERS:
                node_type = col
                id_index = i
            else:
                long_df["ID"].extend([row[id_index] for row in annotation])
                long_df["node_type"].extend([node_type] * len(annotation))
                long_df["annotation_value"].extend([row[i] for row in annotation])
                long_df["col_name"].extend([col] * len(annotation))

        annotation_map = {}
        header_map = {}
        for i in range(len(long_df["ID"])):
            node_id = long_df["ID"][i]
            node_type = long_df["node_type"][i]
            col_name = long_df["col_name"][i]
            anno_value = long_df["annotation_value"][i]

            if node_type not in annotation_map:
                annotation_map[node_type] = {}
            if node_id not in annotation_map[node_type]:
                annotation_map[node_type][node_id] = {}

            if col_name in annotation_map[node_type][node_id]:
                if annotation_map[node_type][node_id][col_name] != anno_value:
                    ValueError(
                        f"Node {node_id} and annotation {col_name} set twice "
                        "in annotation file with ambiguous values."
                    )
            else:
                annotation_map[node_type][node_id][col_name] = str(anno_value)

            if node_type not in header_map:
                header_map[node_type] = {}
            if col_name not in header_map[node_type]:
                # Materials only get new Characteristics, Files only new Comment and Processes only new Parameter Value
                if node_type in DATA_FILE_HEADERS:
                    isa_col_name = f"Comment[{col_name}]"
                elif node_type in MATERIAL_NAME_HEADERS:
                    isa_col_name = f"Characteristics[{col_name}]"
                # elif node_type in PROTOCOL_REF: # Not yet supported and caught above
                # else: # Won't happen since caught above
                header_map[node_type][col_name] = isa_col_name

        return annotation_map, header_map

    def _perform_update(self, isa, annotation_map, header_map):
        # Traverse investigation, studies, assays, potentially updating the nodes.
        visitor = SheetUpdateVisitor(
            annotation_map,
            header_map,
            self.config.force_update,
            self.config.target_study,
            self.config.target_assay,
        )
        iwalker = isa_support.InvestigationTraversal(isa.investigation, isa.studies, isa.assays)
        iwalker.run(visitor)
        investigation, studies, assays = iwalker.build_evolved()

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


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk isa-tab annotate``."""
    return AddAnnotationIsaTabCommand.setup_argparse(parser)
