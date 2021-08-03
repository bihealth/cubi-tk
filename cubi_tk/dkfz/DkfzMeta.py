import attr
from typing import Dict, List, Tuple

import altamisa.isatab.models


@attr.s(auto_attribs=True, frozen=False)
class DkfzMetaArc:
    """Representation of an arc end (i.e. either extremity).
    The actual connection between ISATAB objects is done
    through the ordering in the list of DkfzMetaArc.
    """

    #: Arc end type (currently supported: Material & Process)
    type: str
    #: Name of the object (Material.type or Process.protocol_ref)
    name: str


@attr.s(auto_attribs=True, frozen=False)
class DkfzMetaRowSub:
    """List of meterials & processes from a single Dkfz metafile row, without arcs"""

    #: Materials (as list)
    materials: List[altamisa.isatab.models.Material]
    #: Processes (as list)
    processes: List[altamisa.isatab.models.Process]


@attr.s(auto_attribs=True, frozen=False)
class DkfzMetaRowParsed(DkfzMetaRowSub):
    """Full representation of one single Dkfz metafile row.
    The connection between objects is in the list of arcs.
    This is used when the meta file has been parsed, but mappings
    between ids has not been done.
    Note that the description of arcs is ambiguous between rows,
    but uniquely defined within a row.
    """

    #: List of arcs, defined for a single row
    arcs: List[DkfzMetaArc]


@attr.s(auto_attribs=True, frozen=False)
class DkfzMetaRowMapped(DkfzMetaRowSub):
    """Full representation of one single Dkfz metafile row.
    The arcs represent a connection between two object in
    the graph, labelled by unique identifier strings.
    """

    #: Frozen list of arcs connecting arbitrary elements in the DAG
    arcs: Tuple[str, str]


@attr.s(auto_attribs=True, frozen=False)
class DkfzMetaRow:
    """Storage unit for one Dkfz metafile row, at unparsed, parsed and
    after id mapping stages.
    """

    #: Assay type as labelled in the Dkfz metafile (EXON|RNA|WGS)
    assay_type: str
    #: Unparsed row, as a dict with metafile column names as keys
    row: Dict[str, str]
    #: Parsed row: data assigned to materials, characteristics, ...
    parsed: DkfzMetaRowParsed
    #: Row after id mapping, with references to objects shared by rows
    mapped: DkfzMetaRowMapped


@attr.s(auto_attribs=True, frozen=False)
class DkfzMeta:
    """Representation of the contents of a Dkfz metfile"""

    #: The metafile path. Required to locate fastq files downloaded with the metafile
    filename: str
    #: metafile contents as a dict of DkfzMetaRow with md5 checksums as keys
    content: Dict[str, Dict[str, DkfzMetaRow]]

    @classmethod
    def getValue(
        cls,
        row: DkfzMetaRowSub,
        Material=None,
        Process=None,
        characteristic=None,
        parameter=None,
        comment=None,
        key=None,
    ) -> str:
        """Extract a value from a DkfzMetaRow.
        The returned value is always a string, even for dates, or when the requested element is a list.
        In the latter case, the elements are converted to strings and joined by the semicolumn ";".
        When the requested value is not found in the row (for example no such characteristic),
        the function returns None.
        Usage examples:

        # extract the sample name from the parsed (but unmapped) row:
        sample_name = DkfzMeta.getValue(row=row.parsed, Material="Sample Name", key="name")

        # extract the batch id from the mapped row:
        batch_id = DkfzMeta.getValue(row=row.mapped, Material="Library Name", characteristic="Batch")

        # extract the instrument model from the nucleic acid sequencing process:
        model = DkfzMeta.getValue(row=row.parsed, Process="nucleic acid sequencing EXON", parameter="Instrument Model")
        # Note that some processes require to add the assay type to be recognized.
        # Such processes have a "add_assay_type: yes" in the yaml description.

        The function can be called using **kwargs, when multiple elements are required.
        """
        if Material:
            for m in row.materials:
                if m.type == Material:
                    if key:
                        if key == "name":
                            return m.name
                        else:
                            return None
                    if characteristic:
                        for c in m.characteristics:
                            if c.name == characteristic:
                                return ";".join(map(str, c.value))
                        return None
                    if comment:
                        for c in m.comments:
                            if c.name == comment:
                                return str(c.value)
                        return None
        if Process:
            for p in row.processes:
                if p.protocol_ref == Process:
                    if key:
                        if key == "date":
                            return p.date
                        elif key == "performer":
                            return p.performer
                        else:
                            return None
                    if parameter:
                        for x in p.parameter_values:
                            if x.name == parameter:
                                return ";".join(map(str, x.value))
                        return None
                    if comment:
                        for c in p.comments:
                            if c.name == comment:
                                return str(c.value)
                        return None
        return None
