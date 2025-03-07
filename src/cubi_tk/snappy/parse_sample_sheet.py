"""Common code to parse BioMedSheets"""
import typing
from loguru import logger

from ..isa_support import (
    IsaNodeVisitor,
    first_value,
)
from biomedsheets import io_tsv
from biomedsheets.naming import NAMING_ONLY_SECONDARY_ID
import attr

class ParseSampleSheet:
    """Class contains methods to parse BioMedSheet"""

    #: Whether to look into the largest start batch in family.
    start_batch_in_family: bool = False

    @staticmethod
    def _build_family_max_batch(sheet, batch_key, family_key):
        family_max_batch = {}
        for donor in sheet.bio_entities.values():
            if batch_key in donor.extra_infos and family_key in donor.extra_infos:
                family_id = donor.extra_infos[family_key]
                batch_no = donor.extra_infos[batch_key]
                family_max_batch[family_id] = max(family_max_batch.get(family_id, 0), batch_no)
        return family_max_batch

    def _batch_of(self, donor, family_max_batch, batch_key, family_key):
        if batch_key in donor.extra_infos:
            batch = donor.extra_infos[batch_key]
        else:
            batch = 0
        if self.start_batch_in_family and family_key in donor.extra_infos:
            family_id = donor.extra_infos[family_key]
            batch = max(batch, family_max_batch.get(family_id, 0))
        return batch

    def yield_ngs_library_names(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all NGS library names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            for bio_sample in donor.bio_samples.values():
                for test_sample in bio_sample.test_samples.values():
                    for library in test_sample.ngs_libraries.values():
                        yield library.name

    def yield_ngs_library_names_filtered_by_samples(
        self, sheet, selected_samples, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all NGS library names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param selected_samples: List of sample identifiers as string, e.g., 'P001' instead of 'P001-N1-DNA1-WGS1'.
        :type selected_samples: list

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        for donor in self.yield_donor(sheet=sheet, batch_key=batch_key, family_key=family_key):
            if donor.secondary_id in selected_samples:
                for bio_sample in donor.bio_samples.values():
                    for test_sample in bio_sample.test_samples.values():
                        for library in test_sample.ngs_libraries.values():
                            yield library.name

    def yield_ngs_library_and_folder_names(
        self,
        sheet,
        min_batch=None,
        max_batch=None,
        batch_key="batchNo",
        family_key="familyId",
        selected_ids=None,
    ):
        """Yield all NGS library and folder names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str

        :param selected_ids: List of samples ids to keep, e.g., 'P001' instead of longer library name
        'P001-N1-DNA1-WGS1'. Everything else will be ignored.
        :type selected_ids: list
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            if selected_ids and donor.secondary_id not in selected_ids:
                logger.debug(f"Sample '{donor.secondary_id}' not in provided selected id list.")
                continue
            for bio_sample in donor.bio_samples.values():
                for test_sample in bio_sample.test_samples.values():
                    for library in test_sample.ngs_libraries.values():
                        folder_name = self._get_donor_folder_name(donor) or donor.secondary_id
                        yield library.name, folder_name

    def yield_sample_names(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all sample names (``secondary_id``) from sheet.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            yield donor.secondary_id

    def yield_sample_and_folder_names(
        self,
        sheet,
        min_batch=None,
        max_batch=None,
        batch_key="batchNo",
        family_key="familyId",
        selected_ids=None,
    ):
        """Yield all sample and folder names (``secondary_id``, ``folderName``) from sheet.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str

        :param selected_ids: List of samples ids to keep, e.g., 'P001'. Everything else will be ignored.
        :type selected_ids: list
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            if selected_ids and donor.secondary_id not in selected_ids:
                logger.debug(f"Sample '{donor.secondary_id}' not in provided selected id list.")
                continue
            folder_name = self._get_donor_folder_name(donor) or donor.secondary_id
            yield donor.secondary_id, folder_name

    @staticmethod
    def _get_donor_folder_name(donor):
        """Get folder name

        :param donor: Donor object.
        :type donor: biomedsheets.models.BioEntity

        :return: Returns folder name associated with donor.
        """
        bio_sample = donor.bio_samples.popitem(last=False)[1]
        test_sample = bio_sample.test_samples.popitem(last=False)[1]
        ngs_library = test_sample.ngs_libraries.popitem(last=False)[1]
        return ngs_library.extra_infos.get("folderName")

    def yield_donor(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield donor object from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        family_max_batch = self._build_family_max_batch(sheet, batch_key, family_key)

        # Process all libraries and filter by family batch ID.
        for donor in sheet.bio_entities.values():
            # Ignore below min batch number if applicable
            if min_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch < min_batch:
                    logger.debug(
                        f"Skipping donor '{donor.name}' because '{batch_key}' = {batch} < min_batch = {min_batch}"
                    )
                    continue
            # Ignore above max batch number if applicable
            if max_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch > max_batch:
                    logger.debug(
                        f"Skipping donor '{donor.name}' because '{batch_key}' = {batch} > max_batch = {max_batch}"
                    )
                    # It would be tempting to add a `break`, but there is no guarantee that
                    # the sample sheet is sorted.
                    continue
            yield donor


class SampleSheetBuilder(IsaNodeVisitor):
    def __init__(self):
        #: Source by sample name.
        self.sources = {}
        #: Sample by sample name.
        self.samples = {}
        #: The previous process.
        self.prev_process = None

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_node(process, study, assay)

    def generateSheet(self):
        logger.debug("building sheet")

    def get_libtype(self, splitted_lib, library):
        #get libtype
        lib_type_string = splitted_lib[-1]
        if lib_type_string.startswith("WGS"):
            library_type = "WGS"
        elif lib_type_string.startswith("WES"):
            library_type = "WES"
        elif lib_type_string.startswith("Panel_seq"):
            library_type = "Panel_seq"
        elif lib_type_string.startswith("mRNA_seq"):
            library_type = "mRNA_seq"
        elif lib_type_string.startswith("RNA_seq"):
            library_type = "RNA_seq"
        else:
            raise Exception("Cannot infer library type from %s" % library.name)
        return library_type


####Germline specific Classes, Templates and Constants

#: Template for the to-be-generated file.
HEADER_TPL_GERMLINE = (
    "[Metadata]",
    "schema\tgermline_variants",
    "schema_version\tv1",
    "",
    "[Custom Fields]",
    "key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern",
    "batchNo\tbioEntity\tBatch No.\tinteger\t.\t.\t.\t.\t.",
    "familyId\tbioEntity\tFamily\tstring\t.\t.\t.\t.\t.",
    "projectUuid\tbioEntity\tProject UUID\tstring\t.\t.\t.\t.\t.",
    "libraryKit\tngsLibrary\tEnrichment kit\tstring\t.\t.\t.\t.\t.",
    "",
    "[Data]",
    (
        "familyId\tpatientName\tfatherName\tmotherName\tsex\tisAffected\tlibraryType\tfolderName"
        "\tbatchNo\thpoTerms\tprojectUuid\tseqPlatform\tlibraryKit"
    ),
)

#: Mapping from ISA-tab sex to sample sheet sex.
MAPPING_SEX_GERMLNE = {"female": "F", "male": "M", "unknown": "U", None: "."}

#: Mapping from disease status to sample sheet status.
MAPPING_STATUS_GERMLINE = {"affected": "Y", "carrier": "Y", "unaffected": "N", "unknown": ".", None: "."}

@attr.s(frozen=True, auto_attribs=True)
class SourceGermline:
    family: typing.Optional[str]
    source_name: str
    batch_no: int
    father: str
    mother: str
    sex: str
    affected: str
    sample_name: str


@attr.s(frozen=True, auto_attribs=True)
class SampleGermline:
    source: SourceGermline
    library_name: str
    library_type: str
    folder_name: str
    seq_platform: str
    library_kit: str

class SampleSheetBuilderGermline(SampleSheetBuilder):
    def __init__(self):
        super().__init__()
        self.config = None
        self.project_uuid = ""
        self.first_batch = 0
        self.last_batch = 0

    def set_germline_specific_values(self, config, project_uuid, first_batch, last_batch):
        self.config = config
        self.project_uuid = project_uuid
        self.first_batch = first_batch
        self.last_batch = last_batch

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        source = material_path[0]
        if material.type == "Sample Name" and assay is None:
            sample = material
            characteristics = {c.name: c for c in source.characteristics}
            comments = {c.name: c for c in source.comments}
            batch = characteristics.get("Batch", comments.get("Batch"))
            family = characteristics.get("Family", comments.get("Family"))
            father = characteristics.get("Father", comments.get("Father"))
            mother = characteristics.get("Mother", comments.get("Mother"))
            sex = characteristics.get("Sex", comments.get("Sex"))
            affected = characteristics.get("Disease status", comments.get("Disease status"))
            self.sources[material.name] = SourceGermline(
                family=family.value[0] if family else None,
                source_name=source.name,
                batch_no=batch.value[0] if batch else None,
                father=father.value[0] if father else None,
                mother=mother.value[0] if mother else None,
                sex=sex.value[0] if sex else None,
                affected=affected.value[0] if affected else None,
                sample_name=sample.name,
            )
        elif material.type == "Library Name" or (
            material.type == "Extract Name"
            and self.prev_process.protocol_ref.startswith("Library construction")
        ):
            library = material
            sample = material_path[0]

            splitted_lib = library.name.split("-")
            library_type = self.get_libtype(splitted_lib, library)

            folder_name = first_value("Folder name", node_path)
            if not folder_name:
                folder_name = library.name
            self.samples[sample.name] = SampleGermline(
                source=self.sources[sample.name],
                library_name=library.name,
                library_type=library_type,
                folder_name=folder_name,
                seq_platform=first_value("Platform", node_path),
                library_kit=first_value("Library Kit", node_path),
            )

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_process(process, study, assay)
        self.prev_process = process
        material_path = [x for x in node_path if hasattr(x, "type")]
        sample = material_path[0]
        if process.protocol_ref.startswith("Nucleic acid sequencing"):
            self.samples[sample.name] = attr.evolve(
                self.samples[sample.name], seq_platform=first_value("Platform", node_path)
            )

    def generateSheet(self):
        super().generateSheet()
        result = []
        for sample_name, source in self.sources.items():
            sample = self.samples.get(sample_name, None)
            if not self.config.library_types or not sample or sample.library_type in self.config.library_types:
                row = [
                    source.family or "FAM",
                    source.source_name or ".",
                    source.father or "0",
                    source.mother or "0",
                    MAPPING_SEX_GERMLNE[source.sex.lower()],
                    MAPPING_STATUS_GERMLINE[source.affected.lower()],
                    sample.library_type or "." if sample else ".",
                    sample.folder_name or "." if sample else ".",
                    "0" if source.batch_no is None else source.batch_no,
                    ".",
                    str(self.project_uuid),
                    sample.seq_platform or "." if sample else ".",
                    sample.library_kit or "." if sample else ".",
                ]
                result.append("\t".join([c.strip() for c in row]))

        load_tsv = getattr(io_tsv, "read_%s_tsv_sheet" % "germline")

        sheet = load_tsv(list(HEADER_TPL_GERMLINE) + result, naming_scheme=NAMING_ONLY_SECONDARY_ID)
        parser = ParseSampleSheet()
        samples_in_batch = list(parser.yield_sample_names(sheet, self.first_batch, self.last_batch))
        result = (
            list(HEADER_TPL_GERMLINE)
            + [line if line.split("\t")[1] in samples_in_batch else "#" + line for line in result]
            + [""]
        )
        return result

####Cancer specific Classes, Templates and Constants

HEADER_TPL_CANCER= (
    "[Metadata]",
    "schema\tcancer_matched",
    "schema_version\tv1",
    "",
    "[Custom Fields]",
    "key\tannotatedEntity\tdocs\ttype\tminimum\tmaximum\tunit\tchoices\tpattern",
    "extractionType\ttestSample\textraction type\tstring\t.\t.\t.\t.\t.",
    "libraryKit\tngsLibrary\texome enrichment kit\tstring\t.\t.\t.\t.\t.",
    "",
    "[Data]",
    (
        "patientName\tsampleName\textractionType\tlibraryType\tfolderName\tisTumor\tlibraryKit"
    ),
)

@attr.s(frozen=True, auto_attribs=True)
class SourceCancer:
    source_name: str
    sample_name: str
    is_tumor: str


@attr.s(frozen=True, auto_attribs=True)
class SampleCancer:
    source: SourceCancer
    extraction_type: str
    sample_name_biomed :str
    library_name: str
    library_type: str
    folder_name: str
    library_kit: str

class SampleSheetBuilderCancer(SampleSheetBuilder):
    def __init__(self):
        super().__init__()

    def on_visit_material(self, material, node_path, study=None, assay=None):
        super().on_visit_material(material, node_path, study, assay)
        material_path = [x for x in node_path if hasattr(x, "type")]
        source = material_path[0]
        if material.type == "Sample Name" and assay is None:
            sample = material
            characteristics_material = {c.name: c for c in material.characteristics}
            comments = {c.name: c for c in source.comments}
            tumor =characteristics_material.get("Is tumor", comments.get("Is tumor"))
            self.sources[material.name] = SourceCancer(
                source_name=source.name,
                sample_name=sample.name,
                is_tumor=tumor.value[0] if tumor else None
            )
        elif material.type == "Library Name" or (
            material.type == "Extract Name"
            and self.prev_process.protocol_ref.startswith("Library construction")
        ):
            library = material
            sample = material_path[0]
            splitted_lib = library.name.split("-")
            library_type = self.get_libtype(splitted_lib, library)

            #get extractiontype
            extr_type_string = splitted_lib[-2]
            if extr_type_string.startswith("DNA"):
                extraction_type="DNA"
            elif extr_type_string.startswith("RNA"):
                extraction_type="RNA"
            else:
                raise Exception("Cannot infer exctraction type from %s" % library.name)

            #get sample name for biomedsheet
            sample_name_biomed = splitted_lib[-3]

            folder_name = first_value("Folder name", node_path)
            if not folder_name:
                folder_name = library.name

            self.samples[sample.name] = SampleCancer(
                source=self.sources[sample.name],
                sample_name_biomed = sample_name_biomed,
                extraction_type= extraction_type,
                library_name=library.name,
                folder_name=folder_name,
                library_type=library_type,
                library_kit=first_value("Library Kit", node_path),
            )

    def on_visit_process(self, process, node_path, study=None, assay=None):
        super().on_visit_process(process, study, assay)
        self.prev_process = process
        material_path = [x for x in node_path if hasattr(x, "type")]
        sample = material_path[0]
        if process.protocol_ref.startswith("Nucleic acid sequencing"):
            self.samples[sample.name] = attr.evolve(
                self.samples[sample.name]
            )

    def generateSheet(self):
        super().generateSheet()
        result = []
        #for sample_name, source in self.sources.items():
            #sample = self.samples.get(sample_name, None)
            #if sample:
        for sample_name, sample in self.samples.items():
            source = self.sources.get(sample_name, None)
            row = [
                source.source_name or ".",
                sample.sample_name_biomed or ".",
                sample.extraction_type or "." if sample else ".",
                sample.library_type or "." if sample else ".",
                sample.folder_name or "." if sample else ".",
                source.is_tumor,
                sample.library_kit or "." if sample else ".",
            ]
            result.append("\t".join([c.strip() for c in row]))
        result = (
            list(HEADER_TPL_CANCER)
            + list(result)
            + [""]
        )
        return result
