import csv
import re
from datetime import datetime
from typing import Dict, List, Any, TextIO

from logzero import logger

import altamisa.isatab.models

from .DkfzMeta import DkfzMeta, DkfzMetaRow, DkfzMetaArc, DkfzMetaRowParsed
from .DkfzExceptions import MissingValueError, DuplicateValueError, IllegalValueError


class DkfzMetaParser:
    """Parser of Dkfz metafiles.
    The parser is extensively configurable via the schema provided upon creation.
    """

    UNDETERMINED_PATTERN = re.compile("^Undetermined_([0-9])+.fastq.gz$")

    ILLUMINA_INSTRUMENTS = re.compile(
        "(HI|NOVA|NEXT)SEQ *(500|550|1000|1500|2000|2500|3000|4000|6000|X( *Ten)?)", re.IGNORECASE
    )

    SPECIES = {
        re.compile(
            "([Hh]omo[ _]?[Ss]apiens|hg18|hg19|GRCh37|GRCh38)"
        ): altamisa.isatab.models.OntologyTermRef(
            name="Homo sapiens",
            accession="http://purl.bioontology.org/ontology/NCBITAXON/9606",
            ontology_name="NCBITAXON",
        ),
        re.compile(
            "([Mm]us[ _]?[Mm]usculus|mm9|mm10|GRCm37|GRCm38|GRCm39)"
        ): altamisa.isatab.models.OntologyTermRef(
            name="Mus musculus",
            accession="http://purl.bioontology.org/ontology/NCBITAXON/10090",
            ontology_name="NCBITAXON",
        ),
    }

    def __init__(self, schema):
        self.schema = schema

    def read_meta(self, f: TextIO, ignoreUndetermined=True) -> DkfzMeta:
        """Parses a Dkfz metafile (as io.TextIO object) according to the schema,
        and returns the contents in DkfzMeta.
        """
        reader = csv.DictReader(f, delimiter="\t")
        content = {}
        checksums = []
        for row in reader:
            if "MD5" not in row.keys():
                raise MissingValueError('Missing mandatory column "{}"'.format("MD5"))
            if row["MD5"] in checksums:
                raise DuplicateValueError(
                    "MD5 checksum {} appears multiple time".format(row["MD5"])
                )
            if ignoreUndetermined:
                if "FASTQ_FILE" in row.keys():
                    if DkfzMetaParser.UNDETERMINED_PATTERN.match(row["FASTQ_FILE"]):
                        continue
                else:
                    logger.warning(
                        'Missing column "{}", can\'t test for file of undetermined origin after demultiplexing'.format(
                            "FASTQ_FILE"
                        )
                    )

            if not ("SEQUENCING_TYPE" in row.keys()) or (row["SEQUENCING_TYPE"] is None):
                logger.warning(
                    'Missing column "{}", can\'t extract assay type'.format("SEQUENCING_TYPE")
                )
                continue
            if not row["SEQUENCING_TYPE"] in self.schema["Investigation"]["Assays"].keys():
                logger.warning(
                    'Unknown assay type "{}", data file ignored'.format(row["SEQUENCING_TYPE"])
                )
                continue
            assay_type = row["SEQUENCING_TYPE"]

            checksums.append(row["MD5"])

            if assay_type not in content.keys():
                content[assay_type] = {}
            content[assay_type][row["MD5"]] = self.meta_to_isa(row, assay_type)

        return DkfzMeta(content=content, filename=f.name)

    def meta_to_isa(self, row: Dict[str, str], assay_type="EXON") -> DkfzMetaRow:
        """Parses a single row of a Dkfz metafile (as dict with column names as keys),
        according to the schema, and returns the contents in DkfzMetaRow.
        """
        materials: List[altamisa.isatab.models.Material] = list()
        for theMaterial in self.schema["Assay"]["Material"]:
            materials.append(self._buildMaterial(theMaterial, materials, row))

        processes: List[altamisa.isatab.models.Process] = list()
        for theProcess in self.schema["Assay"]["Process"]:
            processes.append(self._buildProcess(theProcess, processes, row, assay_type))

        processes = DkfzMetaParser._add_library_type(
            processes, assay_type, protocol_ref="library construction" + " " + assay_type
        )
        arcs = self._buildArc(materials, processes, assay_type)

        return DkfzMetaRow(
            assay_type=assay_type,
            row=row,
            parsed=DkfzMetaRowParsed(materials=materials, processes=processes, arcs=arcs),
            mapped=None,
        )

    def _buildMaterial(
        self,
        theMaterial: Dict[str, Any],
        materials: List[altamisa.isatab.models.Material],
        row: Dict[str, str],
    ) -> altamisa.isatab.models.Material:
        """Creates a material by extracting from the row the elements required by the schema"""
        materialType = theMaterial["type"]
        for m in materials:
            if m.name == materialType:
                raise DuplicateValueError("Material {} appears multiple times".format(materialType))

        materialName = self._meta_column_value(row, theMaterial)

        materialCharacteristics: List[altamisa.isatab.models.Characteristics] = list()
        if ("characteristics" in theMaterial.keys()) and not (
            theMaterial["characteristics"] is None
        ):
            for theCharacteristic in theMaterial["characteristics"]:
                theKey = theCharacteristic["name"]
                theValue = self._meta_column_value(row, theCharacteristic)
                DkfzMetaParser._append_to_Characteristics(materialCharacteristics, theKey, theValue)

        materialComments: List[altamisa.isatab.models.Comment] = list()
        if ("comments" in theMaterial.keys()) and not (theMaterial["comments"] is None):
            for theComment in theMaterial["comments"]:
                theKey = theComment["name"]
                theValue = self._meta_column_value(row, theComment)
                DkfzMetaParser._append_to_Comments(materialComments, theKey, theValue)

        return altamisa.isatab.models.Material(
            type=materialType,
            unique_name=materialName,
            name=materialName,
            extract_label="",
            characteristics=tuple(materialCharacteristics),
            comments=tuple(materialComments),
            factor_values=(),
            material_type="",
            headers=list(),
        )

    def _buildProcess(
        self,
        theProcess: Dict[str, Any],
        processes: List[altamisa.isatab.models.Process],
        row: Dict[str, str],
        assay_type: str,
    ) -> altamisa.isatab.models.Process:
        """Creates a process by extracting from the row the elements required by the schema.
        If add_assay_type: yes is present in the schema, the assay type is added to the
        process's protocol ref."""
        processType = theProcess["type"]
        if ("add_assay_type" in theProcess.keys()) and theProcess["add_assay_type"]:
            processType = processType + " " + assay_type
        for p in processes:
            if p.protocol_ref == processType:
                raise DuplicateValueError("Process {} appears multiple times".format(processType))

        processPerformer = self._meta_column_value(row, theProcess, meta_columns="performer")
        processDate = self._meta_column_value(row, theProcess, meta_columns="date")
        processDate = (
            datetime.strptime(processDate, "%Y-%m-%d") if processDate is not None else None
        )

        processParameters: List[altamisa.isatab.models.Process] = list()
        if ("parameters" in theProcess.keys()) and not (theProcess["parameters"] is None):
            for theParameter in theProcess["parameters"]:
                theKey = theParameter["name"]
                theValue = self._meta_column_value(row, theParameter)
                DkfzMetaParser._append_to_Parameters(processParameters, theKey, theValue)

        processComments: List[altamisa.isatab.models.Comment] = list()
        if ("comments" in theProcess.keys()) and not (theProcess["comments"] is None):
            for theComment in theProcess["comments"]:
                theKey = theComment["name"]
                theValue = self._meta_column_value(row, theComment)
                DkfzMetaParser._append_to_Comments(processComments, theKey, theValue)

        return altamisa.isatab.models.Process(
            protocol_ref=processType,
            unique_name="",
            name="",
            name_type="",
            date=processDate,
            performer=processPerformer,
            parameter_values=tuple(processParameters),
            comments=tuple(processComments),
            array_design_ref="",
            first_dimension="",
            second_dimension="",
            headers=list(),
        )

    def _buildArc(
        self,
        materials: List[altamisa.isatab.models.Material],
        processes: List[altamisa.isatab.models.Process],
        assay_type: str,
    ) -> List[DkfzMetaArc]:
        """Build the list of arcs from the schema.
        Each entry in the schema arc list generates an arc between the present entry and the
        former one (except the first of course).
        The function makes sure that all arc extremities are present in the row, and
        amends the process names with the assay type when required.
        Missing arc extremities generate MissingValueError, and unknown types (other than
        Material or Process) generate IllegalValueError.
        """
        arcList: List[DkfzMetaArc, DkfzMetaArc] = list()
        previous: DkfzMetaArc = None
        for arc in self.schema["Assay"]["Arc"]:
            if arc["type"] == "Material":
                current: DkfzMetaArc = None
                for m in materials:
                    if m.type == arc["name"]:
                        current = DkfzMetaArc(type="Material", name=m.type)
                        break
                if current is None:
                    raise MissingValueError(
                        "Missing material {} required in arc".format(arc["name"])
                    )
                if previous is not None:
                    arcList.append((previous, current))
                previous = current
            elif arc["type"] == "Process":
                protocol_ref = arc["name"]
                for p in self.schema["Assay"]["Process"]:
                    if p["type"] == protocol_ref:
                        if ("add_assay_type" in p.keys()) and p["add_assay_type"]:
                            protocol_ref = protocol_ref + " " + assay_type
                        break
                current: DkfzMetaArc = None
                for p in processes:
                    if p.protocol_ref == protocol_ref:
                        current = DkfzMetaArc(type="Process", name=protocol_ref)
                        break
                if current is None:
                    raise MissingValueError(
                        "Missing process {} required in arc".format(arc["name"])
                    )
                if previous is not None:
                    arcList.append((previous, current))
                previous = current
            else:
                raise IllegalValueError("Unknown type {} in arc definition".format(arc["type"]))
        return arcList

    def _meta_column_value(
        self, row: Dict[str, str], theElement: dict, meta_columns: str = "meta_columns"
    ) -> Any:
        """Extract a value from the unparsed row dict, according to the scheme.
        The meta_columns indicate the name of the column(s) to fetch the data from.
        Post-processing the value read from Dkfz metafile is achieved using the "processor" construct.
        Constant values can be input, using the "fixed_value: <constant value>" construct
        in the scheme.

        Usage examples

        # Extract the sample name from the SAMPLE_ID or SAMPLE_NAME columns:
        schema_element = {"type": "Sample Name", "meta_columns": ["SAMPLE_ID", "SAMPLE_NAME"]}
        sample_name = self._meta_column_value(row, schema_element)

        # Extract & post-process the species:
        schema_element = {"type": "Sample Name", "meta_columns": ["SPECIES"], "processor": "get_organism"}
        species = self._meta_column_value(row, schema_element)

        # Extract the sequencing date:
        schema_element = {"type": "nucleic acid sequencing", "date": ["RUN_DATE"]}
        run_date = self._meta_column_value(row, schema_element, meta_columns="date")
        """
        if not (meta_columns in theElement.keys()):
            if "fixed_value" in theElement.keys():
                return theElement["fixed_value"]
            return None
        if theElement[meta_columns] is None:
            return None
        column_names = theElement[meta_columns]
        theValue = {}
        for column_name in column_names:
            if (column_name in row.keys()) and not (row[column_name] is None):
                theValue[column_name] = row[column_name]
        if ("processor" in theElement.keys()) and (not theElement["processor"] is None):
            f = getattr(self, theElement["processor"])
            return f(theValue)
        theValue = set(theValue.values())
        if len(theValue) > 1:
            raise DuplicateValueError(
                "Content of {} is not unique: values {} from metafile columns {}".format(
                    theElement.type, ", ".join(theValue), ", ".join(column_names)
                )
            )
        if len(theValue) == 1:
            return list(theValue)[0]
        if "enforce_present" in theElement.keys() and theElement["enforce_present"]:
            raise MissingValueError("Missing name of material {}".format(theElement["type"]))
        return None

    @staticmethod
    def _add_library_type(
        processes: List[altamisa.isatab.models.Process],
        assay_type: str,
        protocol_ref: str = "library construction",
    ) -> List[altamisa.isatab.models.Process]:
        """Adds 3 parameters which depend on the assay type to the library construction
        process: Library source, Library strategy & Library selection
        """
        theProcess = None
        for p in processes:
            if p.protocol_ref == protocol_ref:
                theProcess = processes.remove(p)
                break

        if theProcess is None:
            theProcess = altamisa.isatab.models.Process(
                protocol_ref=protocol_ref,
                unique_name="",
                name="",
                name_type="",
                date="",
                performer="",
                parameter_values=(),
                comments=(),
                array_design_ref="",
                first_dimension="",
                second_dimension="",
                headers=list(),
            )

        libraryTypes = {}
        if assay_type == "EXON":
            libraryTypes["Library source"] = "GENOMIC"
            libraryTypes["Library strategy"] = "WXS"
            libraryTypes["Library selection"] = "Hybrid selection"
        elif assay_type == "RNA":
            libraryTypes["Library source"] = "TRANSCRIPTOMIC"
            libraryTypes["Library strategy"] = "RNA-Seq"
            libraryTypes["Library selection"] = "PolyA"
        elif assay_type == "WGS":
            libraryTypes["Library source"] = "GENOMIC"
            libraryTypes["Library strategy"] = "WGS"
            libraryTypes["Library selection"] = "RANDOM"
        else:
            raise ValueError("Unknown assay type {}".format(assay_type))

        parameterList: Dict[str, altamisa.isatab.models.ParameterValue] = dict(
            [(x.name, x) for x in theProcess.parameter_values]
        )
        for k, v in libraryTypes.items():
            if k not in parameterList.keys():
                parameterList[k] = altamisa.isatab.models.ParameterValue(name=k, value=[v], unit="")

        theProcess = altamisa.isatab.models.Process(
            protocol_ref=protocol_ref,
            unique_name=theProcess.unique_name,
            name=theProcess.name,
            name_type=theProcess.name_type,
            date=theProcess.date,
            performer=theProcess.performer,
            parameter_values=(list(parameterList.values())),
            comments=theProcess.comments,
            array_design_ref=theProcess.array_design_ref,
            first_dimension=theProcess.first_dimension,
            second_dimension=theProcess.second_dimension,
            headers=theProcess.headers,
        )
        processes.append(theProcess)
        return processes

    @staticmethod
    def _append_to_Characteristics(
        characteristics: List[altamisa.isatab.Characteristics], k: str, v: str
    ):
        """Appends characteristic k value v to the characteristics list.
        Missing values are not added.
        DuplicatedValueError is raised if characteristic k already exists in the list.
        """
        if not v:
            return
        for theCharacteristic in characteristics:
            if theCharacteristic.name == k:
                raise DuplicateValueError("Characteristic {} is not unique".format(k))
        theCharacteristic = altamisa.isatab.Characteristics(name=k, value=[v], unit="")
        characteristics.append(theCharacteristic)

    @staticmethod
    def _append_to_Parameters(parameters: List[altamisa.isatab.ParameterValue], k: str, v: str):
        """Appends parameter k value v to the parameters list.
        Missing values are not added.
        DuplicatedValueError is raised if parameter k already exists in the list.
        """
        if not v:
            return
        for theParameter in parameters:
            if theParameter.name == k:
                raise DuplicateValueError("Parameter {} is not unique".format(k))
        theParameter = altamisa.isatab.ParameterValue(name=k, value=[v], unit="")
        parameters.append(theParameter)

    @staticmethod
    def _append_to_Comments(comments: List[altamisa.isatab.Comment], k: str, v: str):
        """Appends comment k value v to the comments list.
        Missing values are not added.
        DuplicatedValueError is raised if comment k already exists in the list.
        """
        if not v:
            return
        for theComment in comments:
            if theComment.name == k:
                raise DuplicateValueError("Comment {} is not unique".format(k))
        theComment = altamisa.isatab.Comment(name=k, value=v)
        comments.append(theComment)

    def get_organism(self, values):
        """Post-proccess organism value.
        When know (human or mouse), the corresponding ontology term is created.
        The detection of know value is based on the species name, or genome release.
        """
        species = None
        for k, v in values.items():
            for pattern, result in DkfzMetaParser.SPECIES.items():
                m = pattern.search(v)
                if m:
                    if species:
                        raise DuplicateValueError("Multiple species values")
                    species = result
                    break
        if not species:
            logger.warning("Unknown species {}".format(", ".join(values.values())))
        return species

    def get_instrument_model(self, values):
        """Post-proccess instrument model value."""
        model = None
        for k, v in values.items():
            match = DkfzMetaParser.ILLUMINA_INSTRUMENTS.search(v)
            if match:
                if model:
                    raise DuplicateValueError("Multiple Illumina instrument definitions")
                model = match.group(1).lower().capitalize() + "Seq " + match.group(2)
        if not model:
            logger.warning("Unknown Illumina instrument(s) {}".format(", ".join(values.values())))
        return model
