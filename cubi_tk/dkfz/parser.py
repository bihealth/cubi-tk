import re
import sys
from pathlib import Path

import pandas as pd

from logzero import logger

from .models import Assay
from .models import Material
from .models import Protocol
from .models import Characteristics
from .models import Parameter
from .models import Comment
from .DkfzMeta import DkfzMeta

import pdb


class DkfzMetaParser:

    column_names = {
        "FASTQ_FILE": [
            {
                "f": "meta_to_node",
                "args": {"Material": "Assay", "Type": "Name", "Column_name": None},
            },
            {"f": "meta_to_internal", "args": "Filename"},
        ],
        "MATE": [{"f": "meta_to_internal", "args": "Mate"}],
        "READ": [{"f": "meta_to_internal", "args": "Mate"}],
        "MD5": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Characteristics",
                    "Column_name": "Checksum",
                },
            },
            {"f": "meta_to_internal", "args": "Checksum"},
        ],
        "CENTER_NAME": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Center name",
                },
            }
        ],
        "RUN_ID": [],
        "RUN_DATE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Date",
                    "Column_name": None,
                },
            }
        ],
        "LANE_NO": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Lane number",
                },
            }
        ],
        "BASE_COUNT": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "Base count",
                },
            }
        ],
        "READ_COUNT": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "Read count",
                },
            }
        ],
        "CYCLE_COUNT": [],
        "SAMPLE_ID": [
            {
                "f": "meta_to_node",
                "args": {"Material": "Sample", "Type": "Name", "Column_name": None},
            }
        ],
        "SAMPLE_NAME": [
            {
                "f": "meta_to_node",
                "args": {"Material": "Sample", "Type": "Name", "Column_name": None},
            }
        ],
        "BARCODE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Barcode sequence",
                },
            }
        ],
        "INDEX": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Barcode sequence",
                },
            }
        ],
        "SEQUENCING_TYPE": [{"f": "sequencing_type", "args": None}],
        "INSTRUMENT_MODEL": [
            {"f": "instrument_model", "args": None},
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Instrument model",
                },
            },
        ],
        "INSTRUMENT_PLATFORM": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Platform",
                },
            }
        ],
        "PIPELINE_VERSION": [],
        "INSERT_SIZE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "Insert size estimate",
                },
            }
        ],
        "FRAGMENT_SIZE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "Insert size estimate",
                },
            }
        ],
        "LIBRARY_LAYOUT": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Library layout",
                },
            }
        ],
        "SEQUENCING_READ_TYPE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Library layout",
                },
            }
        ],
        "ILSE_NO": [{"f": "meta_to_internal", "args": "Batch"}],
        "COMMENT": [{"f": "meta_to_internal", "args": "Comment"}],
        "LIB_PREP_KIT": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Library kit",
                },
            }
        ],
        "INDEX_TYPE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Barcode kit",
                },
            }
        ],
        "ANTIBODY_TARGET": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Antibody target",
                },
            }
        ],
        "ANTIBODY": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Library construction",
                    "Type": "Parameter",
                    "Column_name": "Antibody",
                },
            }
        ],
        "SEQUENCING_KIT": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Sequencing kit",
                },
            }
        ],
        "PROJECT": [{"f": "test_if_unique", "args": False}],
        "BASECALL_SOFTWARE": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Base caller",
                },
            }
        ],
        "BASE_QUAL_ENCODING": [
            {
                "f": "meta_to_node",
                "args": {
                    "Protocol": "Nucleic acid sequencing",
                    "Type": "Parameter",
                    "Column_name": "Base quality encoding",
                },
            }
        ],
        "SPECIES": [{"f": "species", "args": None}],
        "%_GC_FROM_FASTQC": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "FastQC Per sequence GC content",
                },
            }
        ],
        "LANE_%_PASS_PURITY_FILTER": [],
        "AVERAGE_LANE_Q30": [],
        "LANE_%_BASE_QUAL_MIN30": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Assay",
                    "Type": "Comment",
                    "Column_name": "FastQC Per base sequence quality",
                },
            }
        ],
        "SAMPLE_SUBMISSION_TYPE": [],
        "BIOMATERIAL_ID": [],
        "CUSTOMER_LIBRARY": [],
        "TAGMENTATION_BASED_LIBRARY": [],
        "PATIENT_ID": [
            {
                "f": "meta_to_node",
                "args": {"Material": "Source", "Type": "Name", "Column_name": None},
            }
        ],
        "GENDER": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Source",
                    "Type": "Characteristics",
                    "Column_name": "Sex",
                },
            }
        ],
        "SEX": [
            {
                "f": "meta_to_node",
                "args": {
                    "Material": "Source",
                    "Type": "Characteristics",
                    "Column_name": "Sex",
                },
            }
        ],
        "PHENOTYPE": [{"f": "meta_to_internal", "args": "Phenotype"}],
        "TISSUE_TYPE": [{"f": "meta_to_internal", "args": "Tissue_type"}],
        "DEMULTIPLEX_MISMATCH": [],
        "MASK_ADAPTER": [],
        "BLC2FASTQ_VERSION": [],
        "FASTQ_GENERATOR": [],
        "SEQUENCING_SOFTWARE": [],
        "SASI_EXP_READS": [],
        "SASI_EXP_%": [],
        "SASI_OTHER_READS": [],
        "SASI_OTHER_%": [],
        "BASE_MATERIAL": [],
        "SINGLE_CELL_WELL_LABEL": [],
        "CUSTOMER_TAGS": [{"f": "meta_to_internal", "args": "Tags"}],
    }

    mappings = {
        "SEQUENCING_TYPE_to_Assays": {
            "EXON": "exome",
            "RNA": "transcriptome",
            "WGS": "whole genome",
        }
    }

    known_species = {
        "human": {
            "name": "Homo sapiens",
            "source": "NCBITAXON",
            "url": "http://purl.bioontology.org/ontology/NCBITAXON/9606",
        },
        "mouse": {
            "name": "Mus musculus",
            "source": "NCBITAXON",
            "url": "http://purl.bioontology.org/ontology/NCBITAXON/10090",
        },
    }

    illumina_instruments = re.compile(
        "(HI|NOVA|NEXT)SEQ *(500|550|1000|1500|2000|2500|3000|4000|6000|X( *Ten)?)",
        re.IGNORECASE,
    )

    patterns = {
        "Name": re.compile(".+ Name$"),
        "Characteristics": re.compile(
            "^(Characteristics\\[.+\\]|Term Source REF|Term Accession Number)$"
        ),
        "Parameter": re.compile(
            "^(Parameter\\[.+\\]|Term Source REF|Term Accession Number)$"
        ),
        "Comment": re.compile("^Comment\\[.+\\]$"),
        "fastq_filename": re.compile("^(AS-[0-9]+-LR-[0-9]+)_R([12])\\.fastq\\.gz$"),
    }

    mandatory_columns = {
        "Assay": ["Sample Name", "Extract Name", "Library Name"],
        "Sample": ["Source Name", "Sample Name"],
        "samplesheet": ["Sample Name", "Extract Name", "Library Name"],
    }

    def __init__(self):
        super().__init__()

    def DkfzMeta(
        self, fn, species="human", tsv_shortcut="cancer", ignore_undetermined=True
    ):
        object = DkfzMeta(_from_factory=DkfzMeta._token)

        object.config = {
            "tsv_shortcut": tsv_shortcut if tsv_shortcut else "cancer",
            "species": species if species else "human",
        }

        object.meta_filename = [fn]
        object.meta = [pd.read_table(fn)]
        object.meta[0].fillna("", inplace=True)

        if not "SEQUENCING_TYPE" in object.meta[0].columns:
            raise ValueError(
                "Mandatory column SEQUENCING_TYPE missing from meta file {}".format(fn)
            )
        my_meta = object.meta[0]
        if ignore_undetermined:
            my_meta = my_meta[~my_meta["FASTQ_FILE"].str.contains("Undetermined")]
        sequencing_types = my_meta.groupby("SEQUENCING_TYPE")

        object.assays = {}
        for (k, g) in sequencing_types:
            if k in DkfzMetaParser.mappings["SEQUENCING_TYPE_to_Assays"]:
                v = DkfzMetaParser.mappings["SEQUENCING_TYPE_to_Assays"][k]
                assay = Assay(v)
                for material in ["Source", "Sample", "Extract", "Library", "Assay"]:
                    assay.Materials[material] = Material(material)
                for protocol in [
                    "Sample collection",
                    "Nucleic acid extraction",
                    "Library construction",
                    "Nucleic acid sequencing",
                ]:
                    assay.Protocols[protocol] = Protocol(protocol)
                object.assays[v] = {"meta": g, "isatab": assay, "Internals": {}}

        for (assay_type, assay) in object.assays.items():
            for (j, templates) in DkfzMetaParser.column_names.items():
                for template in templates:
                    f = getattr(self, template["f"])
                    f(
                        object,
                        assay_type=assay_type,
                        meta_column_name=j,
                        args=template["args"],
                    )
            self.meta_to_path(object, assay_type=assay_type)

        for assay in object.assays.values():
            assay["isatab"].set_size(assay["isatab"].Materials["Sample"].size)

        return object

    def meta_to_node(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if not meta_column_name in meta:
            return
        if not args:
            raise ValueError(
                "No destination for metafile column {}".format(meta_column_name)
            )

        if "Material" in args:
            if not args["Material"] in assay.Materials:
                raise ValueError("Unknown material {}".format(args["Material"]))
            material = assay.Materials[args["Material"]]
            if args["Type"] == "Name":
                material.set_values(meta[meta_column_name].tolist())
            elif args["Type"] == "Characteristics":
                characteristic = Characteristics(args["Column_name"])
                characteristic.set_values(
                    meta[meta_column_name].tolist(), category="values"
                )
                material.set_characteristic(characteristic)
            elif args["Type"] == "Comment":
                comment = Comment(args["Column_name"])
                comment.set_values(meta[meta_column_name].tolist(), category="values")
                material.set_comment(comment)
            else:
                raise ValueError(
                    "Type {} incompatible with material".format(args["Type"])
                )

        if "Protocol" in args:
            if not args["Protocol"] in assay.Protocols:
                raise ValueError("Unknown protocol {}".format(args["Protocol"]))
            protocol = assay.Protocols[args["Protocol"]]
            if args["Type"] == "Performer" or args["Type"] == "Date":
                protocol.set_values(
                    meta[meta_column_name].tolist(), category=args["Type"]
                )
            elif args["Type"] == "Parameter":
                parameter = Parameter(args["Column_name"])
                parameter.set_values(meta[meta_column_name].tolist(), category="values")
                protocol.set_parameter(parameter)
            elif args["Type"] == "Comment":
                comment = Comment(args["Column_name"])
                comment.set_values(meta[meta_column_name].tolist(), category="values")
                protocol.set_comment(comment)
            else:
                raise ValueError(
                    "Type {} incompatible with protocol".format(args["Type"])
                )

    def meta_to_internal(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if not meta_column_name in meta:
            return
        object.assays[assay_type]["Internals"][args] = meta[meta_column_name].tolist()

    def meta_to_path(self, object, assay_type):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if (not "RUN_ID" in meta.columns) or (not "FASTQ_FILE" in meta.columns):
            raise ValueError("Missing mandatory columns for setting up the path")

        path = Path(object.meta_filename[0]).parent
        j_fastq_file = meta.columns.tolist().index("FASTQ_FILE")
        j_run_id = meta.columns.tolist().index("RUN_ID")
        file_paths = []
        for i in range(meta.shape[0]):
            m = DkfzMetaParser.patterns["fastq_filename"].match(
                meta.iloc[i, j_fastq_file]
            )
            if not m:
                logger.error(
                    "File {} doesn't match expected pattern".format(
                        meta.iloc[i, j_fastq_file]
                    )
                )
                continue
            p = (
                path
                / meta.iloc[i, j_run_id]
                / m.group(1)
                / "fastq"
                / meta.iloc[i, j_fastq_file]
            )
            if not p.exists():
                # logger.error("Cannot find file {}".format(p))
                # continue
                pass
            file_paths.append(str(p.parent / p.name))

        object.assays[assay_type]["Internals"]["fastq_path"] = file_paths

    def sequencing_type(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        library_source = None
        library_strategy = None
        library_selection = None
        seq_type = meta.iloc[0]["SEQUENCING_TYPE"]
        if seq_type == "EXON":
            library_source = "GENOMIC"
            library_strategy = "WXS"
            library_selection = "Hybrid Selection"
        elif seq_type == "RNA":
            library_source = "TRANSCRIPTOMIC"
            library_strategy = "RNA-Seq"
            library_selection = "PolyA"
        elif seq_type == "WGS":
            library_source = "GENOMIC"
            library_strategy = "WGS"
            library_selection = "RANDOM"
        else:
            return

        protocol = assay.Protocols["Library construction"]

        parameter = Parameter("Library source")
        parameter.set_default(value=library_source, category="values")
        protocol.set_parameter(parameter)

        parameter = Parameter("Library strategy")
        parameter.set_default(value=library_strategy, category="values")
        protocol.set_parameter(parameter)

        parameter = Parameter("Library selection")
        parameter.set_default(value=library_selection, category="values")
        protocol.set_parameter(parameter)

    def test_if_unique(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if not meta_column_name in meta:
            if args:
                raise ValueError(
                    "Column {} contains more than one value (all values should be equal)".format(
                        meta_column_name
                    )
                )
            else:
                return
        unique = True
        value = meta.iloc[0][meta_column_name]
        for x in meta[meta_column_name]:
            unique = unique & (x == value)
        if not unique:
            raise logger.warning(
                "Column {} contains more than one value (all values should be equal)".format(
                    meta_column_name
                )
            )

    def instrument_model(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if not meta_column_name in meta:
            return
        normalized = [""] * meta.shape[0]
        for i in range(meta.shape[0]):
            original = meta.iloc[i][meta_column_name]
            match = DkfzMetaParser.illumina_instruments.search(original)
            if match:
                normalized[i] = (
                    match.group(1).lower().capitalize() + "Seq " + match.group(2)
                )
        meta[meta_column_name] = normalized

    def species(self, object, assay_type, meta_column_name, args):
        meta = object.assays[assay_type]["meta"]
        assay = object.assays[assay_type]["isatab"]

        if object.config["species"] in DkfzMetaParser.known_species:
            material = assay.Materials["Source"]
            organism = Characteristics("Organism")
            organism.set_default(
                value=DkfzMetaParser.known_species[object.config["species"]]["name"],
                category="values",
            )
            organism.set_default(
                value=DkfzMetaParser.known_species[object.config["species"]]["source"],
                category="ref",
            )
            organism.set_default(
                value=DkfzMetaParser.known_species[object.config["species"]]["url"],
                category="accession",
            )
            material.set_characteristic(organism)
        else:
            logger.warning("Unknown species {}".format(args["species"]))
