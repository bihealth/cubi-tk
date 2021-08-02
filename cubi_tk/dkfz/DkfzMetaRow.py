import attr
from typing import Dict, List, Tuple, Any

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

