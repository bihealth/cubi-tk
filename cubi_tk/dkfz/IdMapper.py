import re
import pandas as pd
from typing import Dict, Any

from logzero import logger

import altamisa.isatab.models

from .DkfzMeta import DkfzMeta, DkfzMetaRowSub, DkfzMetaRowMapped
from .DkfzExceptions import IllegalValueError, MissingValueError


class IdMapper:
    """Id mapper, from Dkfz internal ids to ids suitable for snappy & SODAR.

    Example: HCWEB6-B1-D1 is mapped to HCWEB6-N1-DNA1-WES1 & HCWEB6-N1-DNA1-WES2
    for ILSE runs 15275 & 14568 resp.
    Source Name: HCWEB6 -> HCWEB6
    Sample Name: B1 (1st blood sample) -> N1 (1st normal sample)
    Extract Name: D1 (1st DNA extraction) -> DNA1 (1st DNA extraction)
    Library Name: Batch 15275 -> WES1 (1st library construction)
    Library Name: Batch 14568 -> WES2 (2nd library construction)

    During the mapping of the second library construction (ILSE run 14568),
    the mapper is aware of run 15275, and it is able to recognize that the
    same source, sample & extract have been seqeuenced twice.

    To achieve this memory, the mapper aggregates partial structures for
    each separate parsed Dkfz metafile (IdMapper.aggregateMappings).
    Once all files from the same project have been reads in included in the
    mapper, the mapper can produce a mapping table between Dkfz & cubi ids
    (IdMapper.mappingsTable), and it can create a altamisa.isatab.models.Assay
    for each assay type found in any of the Dkfz metafiles. This object
    is a faithful represention of the ISATAB DAG of the assay.

    The mapper is extensively configurable using the yaml schema.
    """

    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        for k, rule in self.schema.items():
            rule["pattern"] = re.compile(rule["pattern"])
            if "replace" in rule.keys() and "mappings" in rule["replace"].keys():
                for m in rule["replace"]["mappings"]:
                    m["when"] = re.compile(m["when"])
        # State variables used for id mapping
        self.mappings = {"items": {}}
        self.metas = list()
        self.unique_materials = {
            "Source Name": {},
            "Sample Name": {},
            "Extract Name": {},
            "Library Name": {},
        }
        self.unique_processes = {}
        self.protocol_refs_n = {}
        self.df = None

    def aggregate_mappings(self, meta: DkfzMeta):
        """Append the contents of meta to the mapper internals.
        This method must be called for every metafile in the study.
        """
        self.metas.append(meta.filename)
        for assay_type, rows in meta.content.items():
            for md5, row in rows.items():
                level_data = self.mappings
                for rule in ["Source", "Sample", "Extract", "Library"]:
                    level_data = IdMapper._perform_mapping(
                        self.schema[rule], row.parsed, level_data, assay_type
                    )
                if "files" not in level_data.keys():
                    level_data["files"] = []
                level_data["files"].append(md5)

    @staticmethod
    def _perform_mapping(
        rule: Dict[str, Any], row: DkfzMetaRowSub, level_data: Dict[str, Any], assay_type: str
    ) -> Dict[str, Any]:
        """Mapping ids at one level (Source/Sample/Extract/Library)
        Each level is inspected, from Source to Library. At every level,
        the Dkfz ID is extracted using a regex pattern applied to a node in the row.
        If a replacement is requested (using the "replace" schema construct),
        a replacement value is extracted from a possibly different node from the same row.
        To avoid id collisions, a serial number can be appended to the replaced (cubi) id,
        using the "increment" schema construct. For example:

        # CUBI id = first group of the pattern extracted from characteristic dkfz_id of Extract:
        Source:
            Material: Extract Name
            characteristic: dkfz_id
            pattern: "^ *[A-z0-9_]+-([A-z0-9_]+)-[A-Z][0-9]+-[A-Z][0-9]+(-[0-9]+)? *$"

        # The Dkfz sample id is defined by another pattern extracted from characteristic dkfz_id of Extract.
        # CUBI id = characteristic isTumor from Sample, followed by serial number:
        Sample:
            Material: Extract Name
            characteristic: dkfz_id
            pattern: "^ *[A-z0-9_]+-[A-z0-9_]+-([A-Z][0-9]+)-[A-Z][0-9]+(-[0-9]+)? *$"
            replace:
                Material: Sample Name
                characteristic: isTumor
                increment: yes

        # The Dkfz library id is defined by the batch characteristic,
        # The CUBI id is taken from the Library strategy parameter.
        # Depending on its value, the CUBI id will take different values,
        # incremented when necessary.
        Library:
            Material: Library Name
            characteristic: Batch
            pattern: "^ *0*([0-9]+) *$"
            replace:
                Process: library construction
                parameter: Library strategy
                increment: yes
                mappings:
                    - when: "WXS"
                      replacement: "WES"
                    - when: "RNA-Seq"
                      replacement: "mRNA_seq"
                    - when: "WGS"
                      replacement: "WGS"
        """
        # Extract Source/Sample/Extract/Library value from the row
        dkfz = IdMapper.extractValue(rule, row)

        # Test if the source/sample/extract/lisrary was already encountered
        if dkfz in level_data["items"].keys():
            return level_data["items"][dkfz]

        # Execute replacement
        if "replace" in rule.keys():
            kwargs = {
                "row": row,
                "Material": None,
                "Process": None,
                "key": None,
                "comment": None,
                "characteristic": None,
                "parameter": None,
            }
            for k in kwargs.keys():
                if k in rule["replace"].keys():
                    kwargs[k] = rule["replace"][k]
                    if k == "Process":
                        kwargs[k] = kwargs[k] + " " + assay_type
            repl = str(DkfzMeta.getValue(**kwargs))
            if "mappings" in rule["replace"].keys():
                cubi = None
                for x in rule["replace"]["mappings"]:
                    if x["when"].match(repl):
                        cubi = x["replacement"]
                        break
                if not cubi:
                    raise MissingValueError(
                        "No replacement value found for {} from {}".format(repl, kwargs)
                    )
            else:
                cubi = repl

            if ("increment" in rule["replace"].keys()) and rule["replace"]["increment"]:
                nMax = 0
                for item in level_data["items"].values():
                    if item["cubi"].startswith(cubi):
                        n = int(IdMapper.remove_prefix(item["cubi"], cubi))
                        if n > nMax:
                            nMax = n
                cubi = cubi + str(nMax + 1)

        else:
            cubi = dkfz

        level_data["items"][dkfz] = {"dkfz": dkfz, "cubi": cubi, "items": {}}
        return level_data["items"][dkfz]

    @staticmethod
    def extractValue(rule, row):
        """Extract one value from the row according to the scheme.
        The rule's regular expression pattern is matched to extract the first group.
        """
        kwargs = {
            "row": row,
            "Material": None,
            "Process": None,
            "key": None,
            "comment": None,
            "characteristic": None,
            "parameter": None,
        }
        for k in kwargs.keys():
            if k in rule.keys():
                kwargs[k] = rule[k]
        value = str(DkfzMeta.getValue(**kwargs))
        if not value:
            raise MissingValueError("No value for element {}".format(kwargs))

        # Extract the sub-part using the pattern
        m = rule["pattern"].match(value)
        if not m:
            raise IllegalValueError("Unexpected value {} from element {}".format(value, kwargs))
        return m.group(rule["group"])

    def extractDkfzId(self, row):
        """Build a unique Dkfz id using all rule elements."""
        parts = []
        for rule in ["Source", "Sample", "Extract", "Library"]:
            v = IdMapper.extractValue(self.schema[rule], row)
            if v is None:
                logger.error("Can't extract {} part of the DKFZ id for file {}".format(rule, row))
                return None
            parts.append(v)
        return "-".join(parts)

    def mappings_table(self):
        """Returns a pandas DataFrame id mapping tables between Dkfz & CUBI ids.
        The Dkfz ids are unique, by default a combination of Dkfz's
        SAMPLE_ID/SAMPLE_NAME & the ILSE_NO.
        CUBI's Source, Sample, Extract & Library ids are also returned.
        """
        df = list()
        sources = self.mappings["items"]
        for dkfz_source in sources.keys():
            cubi_source = sources[dkfz_source]["cubi"]
            samples = sources[dkfz_source]["items"]
            for dkfz_sample in samples.keys():
                cubi_sample = cubi_source + "-" + samples[dkfz_sample]["cubi"]
                extracts = samples[dkfz_sample]["items"]
                for dkfz_extract in extracts.keys():
                    cubi_extract = cubi_sample + "-" + extracts[dkfz_extract]["cubi"]
                    libraries = extracts[dkfz_extract]["items"]
                    for dkfz_library in libraries.keys():
                        cubi_library = cubi_extract + "-" + libraries[dkfz_library]["cubi"]
                        for md5 in libraries[dkfz_library]["files"]:
                            df.append(
                                [
                                    dkfz_source
                                    + "-"
                                    + dkfz_sample
                                    + "-"
                                    + dkfz_extract
                                    + "-"
                                    + dkfz_library,
                                    cubi_source,
                                    cubi_sample,
                                    cubi_extract,
                                    cubi_library,
                                    md5,
                                ]
                            )
        return pd.DataFrame(
            df,
            columns=[
                "dkfz_id",
                "Source Name",
                "Sample Name",
                "Extract Name",
                "Library Name",
                "md5",
            ],
        )

    def apply_mappings(self, meta):
        """Apply the mappings to all rows of a metafile table."""
        df = self.df
        assert all(
            x in df.columns
            for x in ["dkfz_id", "Source Name", "Sample Name", "Extract Name", "Library Name"]
        ), "Missing madatory column"
        df = df[["dkfz_id", "Source Name", "Sample Name", "Extract Name", "Library Name"]]
        df = df.drop_duplicates().set_index("dkfz_id")
        for assay_type, rows in meta.content.items():
            for md5, row in rows.items():
                meta.content[assay_type][md5].mapped = self.apply_mappings_one_row(
                    assay_type, row.parsed, df, md5
                )

    def apply_mappings_one_row(
        self, assay_type: str, row: DkfzMetaRowSub, df: pd.DataFrame, md5: str
    ):
        """Apply the mappings to a single row of a metafile table."""
        # Get the row's unique Dkfz id (with separate parts for the source, sample, ...)
        dkfz = self.extractDkfzId(row)
        if dkfz is None:
            logger.error("Can't map ids for file {}, ignored".format(md5))
            return None
        mappings = None
        try:
            mappings = df.loc[dkfz]
        except KeyError:
            logger.error("DKFZ id {} for file {} not in mappings table, ignored".format(dkfz, md5))
            return None

        # Build a dict of the materials in the row with the levels (source, sample, ...) as keys
        # When the material is not already known, create it.
        # When the material is already known (for example the row refers to a donor for whom
        # another sample is already present), then the contents of the material is checked
        # against the material already stored. If discrepancies are found, a warning is issued.
        materials = {}
        for level in ["Source Name", "Sample Name", "Extract Name", "Library Name"]:
            cubi = mappings[level]
            material = None
            for m in row.materials:
                if m.type == level:
                    material = m
                    break
            m = self._get_unique_material(material, cubi)
            # status = self._is_equal_material(m, material)
            self._is_equal_material(m, material)
            materials[level] = m

        # Complete materials with additional materials in the row (typically the Raw Data File)
        for m in row.materials:
            if m.type not in materials.keys():
                materials[m.type] = m

        # Make a similar dict for processes in the row
        processDict = dict([(p.protocol_ref, p) for p in row.processes])

        # Create arcs between objects in the row, or their equivalent in the store.
        # There is always at least one process between materials, but not two processes
        # can be directly linked by an arc.
        # The procedure below updates the study-wide collections of materials & processes
        # stored in self.unique_materials & self.unique_processes.
        # The tail of each unmapped arc is checked. If it is a process, then
        # the arc is simply added to form a path between two materials.
        # If it is a material, then all processes between the former & current materials
        # are obtained from the list of unique processed (created & added if they didn't
        # exist already). This is done by the self._get_unique_process method. Then the
        # tail material is also added to the store if necessary (self._get_unique_material).
        # All arcs are created with head & tails referring to unique DAG objects.
        arcs = list()
        materialBefore = None
        processesBetween = list()
        processes = list()
        arcHead = None
        arcTail = None
        for a in row.arcs:
            if a[1].type == "Material":
                if not a[1].name in materials.keys():
                    raise IllegalValueError(
                        "Unknown material {} for arc in {}".format(a[1].name, dkfz)
                    )
                materialAfter = materials[a[1].name].name
                for process in processesBetween:
                    p = self._get_unique_process(process, materialBefore, materialAfter)
                    # status = self._is_equal_process(p, process, dkfz)
                    self._is_equal_process(p, process, dkfz)
                    processes.append(p)
                    arcTail = p.unique_name
                    arcs.append(altamisa.isatab.models.Arc(head=arcHead, tail=arcTail))
                    arcHead = arcTail
                processesBetween = list()
                arcTail = materialAfter
                arcs.append(altamisa.isatab.models.Arc(head=arcHead, tail=arcTail))
                arcHead = arcTail
                materialBefore = materialAfter
            elif a[1].type == "Process":
                if not materialBefore:
                    if a[0].type != "Material" or a[0].name != "Source Name":
                        raise IllegalValueError("Missing source in arcs")
                    materialBefore = materials[a[0].name].name
                    arcHead = materialBefore
                processesBetween.append(processDict[a[1].name])
            else:
                raise IllegalValueError("Unknown arc end-point type {}".format(a[1].type))

        return DkfzMetaRowMapped(materials=list(materials.values()), processes=processes, arcs=arcs)

    def _get_unique_process(
        self, process: altamisa.isatab.models.Process, materialBefore: str, materialAfter: str
    ) -> altamisa.isatab.models.Process:
        """Extract from the store self.unique_processes the unique process which corresponds
        to the process argument. If this process is not yet in the store, then create it &
        add it to the store. The process's serial number (stored in self.protocol_refs_n) is updated.
        """
        unique_name = materialBefore + " -> " + process.protocol_ref + " -> " + materialAfter
        if unique_name not in self.unique_processes.keys():
            if process.protocol_ref not in self.protocol_refs_n.keys():
                self.protocol_refs_n[process.protocol_ref] = 0
            n = self.protocol_refs_n[process.protocol_ref] + 1
            self.protocol_refs_n[process.protocol_ref] = n
            p = altamisa.isatab.models.Process(
                protocol_ref=process.protocol_ref,
                unique_name=process.protocol_ref + str(n),
                name=unique_name,
                name_type=process.name_type,
                date=process.date,
                performer=process.performer,
                parameter_values=process.parameter_values,
                comments=process.comments,
                array_design_ref=process.array_design_ref,
                first_dimension=process.first_dimension,
                second_dimension=process.second_dimension,
                headers=process.headers,
            )
            self.unique_processes[unique_name] = p
        return self.unique_processes[unique_name]

    def _is_equal_process(self, p, process, dkfz_id):
        """Test equality between processes. Parameters & comments lists are also checked."""
        status = True
        if p.performer != process.performer:
            status = False
            logger.warning(
                "Different performer for protocol_ref {} ({}/{}) of {}: Stored = {}, New = {}".format(
                    p.protocol_ref, p.name, process.name, dkfz_id, p.performer, process.performer
                )
            )
        if p.date != process.date:
            status = False
            logger.warning(
                "Different performer for protocol_ref {} ({}/{}) of {}: Stored = {}, New = {}".format(
                    p.protocol_ref, p.name, process.name, dkfz_id, str(p.date), str(process.date)
                )
            )
        if not IdMapper._is_equal_CharactOrParam(p.parameter_values, process.parameter_values):
            status = False
            logger.warning(
                "Different parameter values for {} ({}/{}) {}".format(
                    p.protocol_ref, p.name, process.name, dkfz_id
                )
            )
            logger.warning(
                "    Stored: {}".format(", ".join([str(v.__dict__) for v in p.parameter_values]))
            )
            logger.warning(
                "    New:    {}".format(
                    ", ".join([str(v.__dict__) for v in process.parameter_values])
                )
            )
        if p.comments != process.comments:
            status = False
            logger.warning(
                "Different comments for {} ({}/{}) {}".format(
                    p.protocol_ref, p.name, process.name, dkfz_id
                )
            )
            logger.warning(
                "    Stored: {}".format(", ".join([str(c.__dict__) for c in p.comments]))
            )
            logger.warning(
                "    New:    {}".format(", ".join([str(c.__dict__) for c in process.comments]))
            )
        return status

    def _get_unique_material(self, material, cubi):
        """Extract from the store self.unique_materials the unique material which corresponds
        to the material argument. If this material is not yet in the store, then create it &
        add it to the store. The newly created material has the CUBI id as name & unique_name.
        """
        level = material.type
        if cubi not in self.unique_materials[level].keys():
            m = altamisa.isatab.models.Material(
                type=material.type,
                unique_name=cubi,
                name=cubi,
                extract_label=material.extract_label,
                characteristics=material.characteristics,
                comments=material.comments,
                factor_values=material.factor_values,
                material_type=material.material_type,
                headers=material.headers,
            )
            self.unique_materials[level][cubi] = m
        return self.unique_materials[level][cubi]

    def _is_equal_material(self, m, material):
        """Test equality between materials. Characteristics, factors & comments lists are also checked."""
        status = True
        if not IdMapper._is_equal_CharactOrParam(m.characteristics, material.characteristics):
            status = False
            logger.warning("Different characteristics for {}/{}".format(m.name, material.name))
            logger.warning(
                "    Stored: {}".format(", ".join([str(c.__dict__) for c in m.characteristics]))
            )
            logger.warning(
                "    New:    {}".format(
                    ", ".join([str(c.__dict__) for c in material.characteristics])
                )
            )
        if m.factor_values != material.factor_values:
            status = False
            logger.warning("Different factor values for {}/{}".format(m.name, material.name))
            logger.warning(
                "    Stored: {}".format(", ".join([str(v.__dict__) for v in m.factor_values]))
            )
            logger.warning(
                "    New:    {}".format(
                    ", ".join([str(v.__dict__) for v in material.factor_values])
                )
            )
        if m.comments != material.comments:
            status = False
            logger.warning("Different comments for {}/{}".format(m.name, material.name))
            logger.warning(
                "    Stored: {}".format(", ".join([str(c.__dict__) for c in m.comments]))
            )
            logger.warning(
                "    New:    {}".format(", ".join([str(c.__dict__) for c in material.comments]))
            )
        return status

    @staticmethod
    def _is_equal_CharactOrParam(list1, list2):
        """Test if two characteristics or two parameter values are identical.
        Also works for value lists.
        """
        if len(list1) != len(list2):
            return False
        if len(list1) == 0:
            return True
        if not isinstance(list1[0], type(list2[0])):
            return False
        names1 = set([x.name for x in list1])
        names2 = set([x.name for x in list2])
        if names1 != names2:
            return False
        for el1 in list1:
            for el2 in list2:
                if el1.name != el2.name:
                    continue
                if el1.unit != el2.unit or set(el1.value) != set(el2.value):
                    return False
        return True

    @staticmethod
    def remove_prefix(text, prefix):
        """Removes a prefix from a string"""
        return text[text.startswith(prefix) and len(prefix) :]
