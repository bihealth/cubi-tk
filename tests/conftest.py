"""Shared fixtures for the unit tests"""

import io
import textwrap

from biomedsheets.io_tsv import read_germline_tsv_sheet, read_cancer_tsv_sheet
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
from biomedsheets.shortcuts import GermlineCaseSheet, CancerCaseSheet

import pytest
from loguru import logger
from _pytest.logging import LogCaptureFixture

@pytest.fixture
def caplog(caplog: LogCaptureFixture):
    handler_id = logger.add(
        caplog.handler,
        format="{message}",
        level=0,
        filter=lambda record: record["level"].no >= caplog.handler.level,
        enqueue=False,  # Set to 'True' if your test is spawning child processes.
    )
    yield caplog
    logger.remove(handler_id)


@pytest.fixture
def minimal_config():
    """Return configuration text"""
    return textwrap.dedent(
        r"""
        static_data_config: {}

        step_config: {}

        data_sets:
          first_batch:
            sodar_uuid: 466ab946-ce6a-4c78-9981-19b79e7bbe86
            file: sheet.tsv
            search_patterns:
            - {'left': '*/*/*_R1.fastq.gz', 'right': '*/*/*_R2.fastq.gz'}
            search_paths: ['/path']
            type: germline_variants
            naming_scheme: only_secondary_id
        """
    ).lstrip()


@pytest.fixture
def germline_trio_sheet_tsv():
    """Return contents for germline trio plus TSV file"""
    return textwrap.dedent(
        """
        [Metadata]
        schema\tgermline_variants
        schema_version\tv1

        [Custom Fields]
        key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern
        batchNo\tbioEntity\tBatch No.\tinteger\t.\t.\t.\t.\t.
        familyId\tbioEntity\tFamily\tstring\t.\t.\t.\t.\t.
        projectUuid\tbioEntity\tProject UUID\tstring\t.\t.\t.\t.\t.
        libraryKit\tngsLibrary\tEnrichment kit\tstring\t.\t.\t.\t.\t.

        [Data]
        familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit
        FAM_index\tindex\tfather\tmother\tM\tY\tWES\tindex\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tfather\t0\t0\tM\tN\tWES\tfather\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        FAM_index\tmother\t0\t0\tF\tN\tWES\tmother\t1\t.\t466ab946-ce6a-4c78-9981-19b79e7bbe86\tIllumina\tAgilent SureSelect Human All Exons V6r2
        """
    ).lstrip()

@pytest.fixture
def cancer_sheet_tsv():
    """Return contents for germline trio plus TSV file"""
    return textwrap.dedent(
        """
        [Metadata]
        schema\tcancer_matched
        schema_version\tv1

        [Custom Fields]
        key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern
        extractionType\ttestSample\textraction type\tstring\t.\t.\t.\t.\t.
        libraryKit\tngsLibrary\texome enrichment kit\tstring\t.\t.\t.\t.\t.

        [Data]
        patientName\tsampleName\textractionType\tlibraryType\tfolderName\tisTumor\tlibraryKit
        patient1\tN1\tDNA\tWES\tpatient1-N1-DNA1-WES1\tN\tAgilent SureSelect Human All Exon V8
        patient1\tT1\tDNA\tWES\tpatient1-T1-DNA1-WES1\tY\tAgilent SureSelect Human All Exon V8
        patient2\tN1\tDNA\tWES\tpatient2-N1-DNA1-WES1\tN\tAgilent SureSelect Human All Exon V8
        patient2\tT1\tDNA\tWES\tpatient2-T1-DNA1-WES1\tY\tAgilent SureSelect Human All Exon V8
        """
    ).lstrip()


@pytest.fixture
def germline_trio_sheet_object(germline_trio_sheet_tsv):
    """Returns GermlineCaseSheet object with trio cohort."""
    # Create dna sample sheet based on germline sheet
    germline_sheet_io = io.StringIO(germline_trio_sheet_tsv)
    return GermlineCaseSheet(
        sheet=read_germline_tsv_sheet(germline_sheet_io, naming_scheme=NAMING_ONLY_SECONDARY_ID)
    )


@pytest.fixture
def cancer_sheet_object(cancer_sheet_tsv):
    """Returns CancerCaseSheet object with trio cohort."""
    # Create dna sample sheet based on cancer sheet
    cancer_sheet_io = io.StringIO(cancer_sheet_tsv)
    return CancerCaseSheet(
        sheet=read_cancer_tsv_sheet(cancer_sheet_io, naming_scheme=NAMING_ONLY_SECONDARY_ID)
    )


@pytest.fixture
def mock_toml_config():
    return textwrap.dedent(
        """
        [global]
        sodar_server_url = "https://sodar.bihealth.org/"
        sodar_api_token = "token123"
        """
    ).lstrip()


def my_exists(self):
    """Method is used to patch pathlib.Path.exists"""
    # self is the Path instance, str(Path) returns the path string
    return str(self) == "/base/path/.snappy_pipeline"


def my_get_sodar_info(_self, sodar_api = None):
    """Method is used to patch cubi_tk.snappy.itransfer_common.SnappyItransferCommandBase.get_sodar_info"""
    return "466ab946-ce6a-4c78-9981-19b79e7bbe86", "/irods/dest"


def my_sodar_api_export(n_assays=1, offset =0):
    """Return contents for api.samplesheet.export"""
    assay = textwrap.dedent(
        """
        Sample Name\tProtocol REF\tParameter Value[Concentration measurement]\tPerformer\tDate\tExtract Name\tCharacteristics[Concentration]\tUnit\tTerm Source REF\tTerm Accession Number\tProtocol REF\tParameter Value[Provider name]\tParameter Value[Provider contact]\tParameter Value[Provider project ID]\tParameter Value[Provider sample ID]\tParameter Value[Provider QC status]\tParameter Value[Requestor contact]\tParameter Value[Requestor project]\tParameter Value[Requestor sample ID]\tParameter Value[Concentration measurement]\tParameter Value[Library source]\tParameter Value[Library strategy]\tParameter Value[Library selection]\tParameter Value[Library layout]\tParameter Value[Library kit]\tComment[Library kit catalogue ID]\tParameter Value[Target insert size]\tParameter Value[Wet-lab insert size]\tParameter Value[Barcode kit]\tParameter Value[Barcode kit catalogue ID]\tParameter Value[Barcode name]\tParameter Value[Barcode sequence]\tPerformer\tDate\tLibrary Name\tCharacteristics[Folder name]\tCharacteristics[Concentration]\tUnit\tTerm Source REF\tTerm Accession Number\tProtocol REF\tParameter Value[Platform]\tParameter Value[Instrument model]\tParameter Value[Base quality encoding]\tParameter Value[Center name]\tParameter Value[Center contact]\tPerformer\tDate\tRaw Data File
        Sample1-N1\tNucleic acid extraction WES\t\t\t\tSample1-N1-DNA1\t\t\t\t\tLibrary construction WES\t\t\t\t\t\t\t\t\t\tGENOMIC\tWXS\tHybrid Selection\tPAIRED\tAgilent SureSelect Human All Exon V7\t\t\t\t\t\t\t\t\t\tSample1-N1-DNA1-WES1\tFolder1\t\t\t\t\tNucleic acid sequencing WES\tILLUMINA\tIllumina NovaSeq 6000\tPhred+33
        Sample2-N1\tNucleic acid extraction WES\t\t\t\tSample2-N1-DNA1\t\t\t\t\tLibrary construction WES\t\t\t\t\t\t\t\t\t\tGENOMIC\tWXS\tHybrid Selection\tPAIRED\tAgilent SureSelect Human All Exon V7\t\t\t\t\t\t\t\t\t\tSample2-N1-DNA1-WES1\tFolder2\t\t\t\t\tNucleic acid sequencing WES\tILLUMINA\tIllumina NovaSeq 6000\tPhred+33
        Sample3-N1\tNucleic acid extraction WES\t\t\t\tSample3-N1-DNA1\t\t\t\t\tLibrary construction WES\t\t\t\t\t\t\t\t\t\tGENOMIC\tWXS\tHybrid Selection\tPAIRED\tAgilent SureSelect Human All Exon V7\t\t\t\t\t\t\t\t\t\tSample3-N1-DNA1-WES1\tFolder3\t\t\t\t\tNucleic acid sequencing WES\tILLUMINA\tIllumina NovaSeq 6000\tPhred+33
        """
    ).lstrip()

    isa_dict = {
        "investigation": {"path": "i_Investigation.txt", "tsv": None},
        "studies": {"s_Study_%d.txt" % offset : {"tsv": ""}},
        "assays": {"a_name_%d" % offset: {"tsv": assay}},
    }
    if n_assays > 1:
        for i in range(1+offset, n_assays+offset):
            isa_dict["assays"]["a_name_%d" % i] = {"tsv": assay}

    return isa_dict
