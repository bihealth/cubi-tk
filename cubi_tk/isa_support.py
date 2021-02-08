"""Helper code for working with altamisa objects."""

from itertools import chain
from pathlib import Path
import tempfile
import typing

import attr
from altamisa.isatab import (
    InvestigationInfo,
    Study,
    Assay,
    InvestigationReader,
    StudyReader,
    AssayReader,
)
from logzero import logger


@attr.s(frozen=True, auto_attribs=True)
class IsaData:
    """Bundle together investigation, studies, assays from one project."""

    #: Investigation.
    investigation: InvestigationInfo
    #: Investigation file name.
    investigation_filename: str
    #: Tuple of studies.
    studies: typing.Dict[str, Study]
    #: Tuple of assays.
    assays: typing.Dict[str, Assay]


def load_investigation(i_path: typing.Union[str, Path]) -> IsaData:
    """Load investigation information from investigation files.

    Study and assay files are expected to be next to the investigation file.
    """
    i_path = Path(i_path)
    with i_path.open("rt") as i_file:
        investigation = InvestigationReader.from_stream(
            input_file=i_file, filename=i_path.name
        ).read()

    studies = {}
    assays = {}
    for study in investigation.studies:
        with (i_path.parent / study.info.path).open() as s_file:
            studies[study.info.path.name] = StudyReader.from_stream(
                study_id=study.info.path.name, input_file=s_file
            ).read()
            for assay in study.assays:
                with (i_path.parent / assay.path).open() as a_file:
                    assays[assay.path.name] = AssayReader.from_stream(
                        study_id=studies[study.info.path.name].file.name,
                        assay_id=assay.path.name,
                        input_file=a_file,
                    ).read()

    return IsaData(investigation, str(i_path), studies, assays)


def isa_dict_to_isa_data(isa_dict):
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        i_path = Path(isa_dict["investigation"]["path"])
        with (tmp_path / i_path.name).open("wt") as out_f:
            out_f.write(isa_dict["investigation"]["tsv"])
        for path, tsv in isa_dict["studies"].items():
            with (tmp_path / path).open("wt") as out_f:
                out_f.write(tsv["tsv"])
        for path, tsv in isa_dict["assays"].items():
            with (tmp_path / path).open("wt") as out_f:
                out_f.write(tsv["tsv"])
        return load_investigation(tmp_path / i_path.name)


#: Constant representing materials.
TYPE_MATERIAL = "MATERIAL"
#: Constant representing processes.
TYPE_PROCESS = "PROCESS"
#: Constant representing arcs.
TYPE_ARC = "ARC"


class IsaGraph:
    """Helper class representing an ISA study or assay as a DAG.

    The DAG is stored in an "expanded" (each material, process and "arc" is a node, connected
    by artificial arcs) "forward-star" (all outgoing arcs are stored for each node) and
    "reverse-star" (all incoming arcs) representation.  This allows for a simpler implementations
    of DFS and BFS as well as easily finding all nodes with in-degree and out-degree of 0.
    """

    def __init__(self, materials, processes, arcs):
        #: The Material entries, by their unique ID.
        self.materials = dict(materials)
        #: The Process entries, by their unique ID.
        self.processes = dict(processes)
        #: The arcs.
        self.arcs = tuple(arcs)
        forward, reverse = self._build_graphs(self.materials, self.processes, self.arcs)
        #: Expanded forward star representation.
        self.forward = forward
        #: Expanded reverse star representation.
        self.reverse = reverse
        #: Nodes without incoming edges.
        self.starts = tuple(k for k, vs in reverse.items() if not vs)
        #: Nodes without outgoing edges.
        self.ends = tuple(k for k, vs in forward.items() if not vs)
        #: Label each node with its original object.
        self._node_objs = dict(
            enumerate(chain(self.materials.values(), self.processes.values(), self.arcs))
        )
        #: Label each node with is original ID.
        self._node_ids = dict(
            enumerate(chain(self.materials.keys(), self.processes.keys(), range(len(self.arcs))))
        )
        #: All material node numbers by material name.
        self.mat_node_by_name = {m.name: m_no for m_no, m in enumerate(self.materials.values())}
        #: Label each node with its type.
        self._node_types = dict(
            enumerate(
                chain(
                    [TYPE_MATERIAL] * len(self.materials),
                    [TYPE_PROCESS] * len(self.processes),
                    [TYPE_ARC] * len(self.arcs),
                )
            )
        )

    def _build_graphs(self, materials, processes, arcs):
        forward = {i: set() for i in range(len(materials) + len(processes) + len(arcs))}
        reverse = {i: set() for i in range(len(materials) + len(processes) + len(arcs))}
        # Build mappings from material/process ID to node number, arcs will be traversed so we
        # do not need a mapping here.
        id_to_node = {
            **{m_id: m_no for m_no, m_id in enumerate(materials.keys())},
            **{p_id: p_no for p_no, p_id in enumerate(processes.keys(), len(materials))},
        }
        for a_no, arc in enumerate(arcs, len(id_to_node)):
            forward[id_to_node[arc.tail]].add(a_no)
            forward[a_no].add(id_to_node[arc.head])
            reverse[id_to_node[arc.head]].add(a_no)
            reverse[a_no].add(id_to_node[arc.tail])
        return (
            {k: tuple(sorted(vs)) for k, vs in forward.items()},
            {k: tuple(sorted(vs)) for k, vs in reverse.items()},
        )

    def dfs(self, start=None, order="pre"):
        """Perform depth first search on graph, either from a particular start node or from all.

        The results are yielded as ``(type, obj)``.  The order can be controlled by setting
        ``order`` to one of ``pre`` or ``post``.
        """
        seen = set()
        path = []
        if start is not None:
            logger.debug("starting from one node %s", start)
            yield from self._dfs(start, seen, order, path)
        else:
            logger.debug("starting from all nodes")
            for s in self.starts:
                logger.debug("starting from current node %s", s)
                yield from self._dfs(s, seen, order, path)

    def _dfs(self, curr, seen, order, path):
        if self._node_types[curr] in (TYPE_MATERIAL, TYPE_PROCESS):
            path.append(self._node_objs[curr])
        if curr in seen:
            return
        else:
            seen.add(curr)
        if order == "pre":
            yield curr, self._node_types[curr], self._node_objs[curr], tuple(path)
        for other in self.forward[curr]:
            yield from self._dfs(other, seen, order, path)
        if order != "pre":
            yield curr, self._node_types[curr], self._node_objs[curr], tuple(path)
        if self._node_types[curr] in (TYPE_MATERIAL, TYPE_PROCESS):
            path.pop()


class IsaNodeVisitor:
    """Base class for a visitor as used in the InvestigationTraversal, StudyTraversal, and
    AssayTraversal classes.
    """

    def on_begin_investigation(self, investigation):
        logger.debug("begin investigation %s", investigation.info.path)

    def on_end_investigation(self, investigation):
        logger.debug("end investigation %s", investigation.info.path)

    def on_begin_study(self, investigation, study):
        _ = investigation
        logger.debug("begin study %s", study.file)

    def on_end_study(self, investigation, study):
        _ = investigation
        logger.debug("end study %s", study.file)

    def on_begin_assay(self, investigation, study, assay):
        _, _ = investigation, study
        logger.debug("begin assay %s", assay.file)

    def on_end_assay(self, investigation, study, assay):
        _, _ = investigation, study
        logger.debug("end assay %s", assay.file)

    def on_traverse_arc(self, arc, node_path, study=None, assay=None):
        _, _, _, = node_path, study, assay
        logger.debug("traversing arc %s", arc)

    def on_visit_node(self, node, node_path, study=None, assay=None):
        _, _, _, = node_path, study, assay
        logger.debug("visiting node %s", node)

    def on_visit_material(self, material, node_path, study=None, assay=None):
        _, _, _, = node_path, study, assay
        logger.debug("visiting material %s", material)

    def on_visit_process(self, process, node_path, study=None, assay=None):
        _, _, _, = node_path, study, assay
        logger.debug("visiting process %s", process)


class InvestigationTraversal:
    """Allow for easy traversal of an investigation.

    If node visitors return a value that is not None, it is expected to a new node that
    replaces the old (the internal unique ID must not be changed such that arcs remain).
    """

    def __init__(self, investigation, studies, assays):
        #: Investigation object.
        self.investigation = investigation
        #: Mapping from study name (file name) to Study
        self.studies = dict(studies)
        if len(self.studies) > 1:
            raise Exception("Only one study supported")
        #: Mapping from assay name (file name) to Assay
        self.assays = assays
        if len(self.assays) > 1:
            raise Exception("Only one assay supported")
        # Study traversal objects.
        self._study_traversals = {}

    def gen(self, visitor: IsaNodeVisitor):
        logger.debug("start investigation traversal")
        visitor.on_begin_investigation(self.investigation)
        for file_name, study in self.studies.items():
            logger.debug("create study traversal %s", file_name)
            st = StudyTraversal(self, study, self.assays)
            self._study_traversals[file_name] = st
            yield from st.gen(visitor)
            logger.debug("finalize study traversal %s", file_name)
        visitor.on_end_investigation(self.investigation)
        logger.debug("end investigation traversal")

    def run(self, visitor: IsaNodeVisitor):
        return tuple(self.gen(visitor))

    def build_evolved(
        self,
    ) -> typing.Tuple[InvestigationInfo, typing.Dict[str, Study], typing.Dict[str, Assay]]:
        """Return study with updated materials and processes."""
        studies = {}
        assays: typing.Dict[typing.Any, typing.Any] = {}
        for key, st in self._study_traversals.items():
            studies[key] = st.build_evolved_study()
            assays.update(st.build_evolved_assays())
        return self.investigation, studies, assays


class StudyTraversal:
    """Allow for easy traversal of a study."""

    def __init__(self, investigation, study, assays):
        self.investigation = investigation
        self.study = study
        self.assays = assays
        self.isa_graph = IsaGraph(self.study.materials, self.study.processes, self.study.arcs)
        self.assay_traversals = {
            key: AssayTraversal(investigation, study, assay) for key, assay in assays.items()
        }
        self._materials = {}
        self._processes = {}

    def gen(self, visitor: IsaNodeVisitor, start_name=None):
        logger.debug("start study traversal %s", self.study.file)
        func_mapping: typing.Dict[str, typing.Tuple[typing.Callable, ...]] = {
            TYPE_ARC: (visitor.on_traverse_arc,),
            TYPE_MATERIAL: (visitor.on_visit_node, visitor.on_visit_material),
            TYPE_PROCESS: (visitor.on_visit_node, visitor.on_visit_process),
        }
        if start_name:
            dfs_start = self.isa_graph.mat_node_by_name[start_name]
        else:
            dfs_start = None
        visitor.on_begin_study(self.investigation, self.study)
        # Visit all ISA nodes in DFS fashion.  For each node, register it in visitor, yield
        # information to caller, and potentially start DFS through all assays containing the
        # sample.
        for node_id, obj_type, obj, node_path in self.isa_graph.dfs(dfs_start):
            new_obj = obj
            for func in func_mapping[obj_type]:
                tmp_obj = func(obj, node_path=node_path, study=self.study)
                new_obj = tmp_obj or new_obj
            if obj_type == TYPE_MATERIAL:
                self._materials[obj.unique_name] = new_obj
            elif obj_type == TYPE_PROCESS:
                self._processes[obj.unique_name] = new_obj
            yield "study", self.study, obj_type, obj
            if node_id in self.isa_graph.ends:
                assert obj_type == TYPE_MATERIAL
                for assay_traversal in self.assay_traversals.values():
                    if obj.name in assay_traversal.isa_graph.mat_node_by_name:
                        logger.debug(
                            "jumping into assay %s, starting from %s",
                            assay_traversal.assay.file,
                            obj.name,
                        )
                        yield from assay_traversal.gen(visitor, start_name=obj.name)
        visitor.on_end_study(self.investigation, self.study)
        logger.debug("end study traversal %s", self.study.file)

    def run(self, visitor: IsaNodeVisitor, start_name=None):
        return tuple(self.gen(visitor, start_name))

    def build_evolved_study(self) -> Study:
        """Return study with updated materials and processes."""
        return attr.evolve(self.study, materials=self._materials, processes=self._processes)

    def build_evolved_assays(self) -> typing.Dict[str, Assay]:
        """Return tuple of evolved assays."""
        return {key: value.build_evolved_assay() for key, value in self.assay_traversals.items()}


class AssayTraversal:
    """Allow for easy traversal of assay."""

    def __init__(self, investigation, study, assay):
        self.investigation = investigation
        self.study = study
        self.assay = assay
        self.isa_graph = IsaGraph(self.assay.materials, self.assay.processes, self.assay.arcs)
        self._materials = {}
        self._processes = {}

    def gen(self, visitor: IsaNodeVisitor, start_name=None):
        logger.debug("start assay traversal %s", self.assay.file)
        func_mapping: typing.Dict[str, typing.Tuple[typing.Callable, ...]] = {
            TYPE_ARC: (visitor.on_traverse_arc,),
            TYPE_MATERIAL: (visitor.on_visit_node, visitor.on_visit_material),
            TYPE_PROCESS: (visitor.on_visit_node, visitor.on_visit_process),
        }
        if start_name:
            dfs_start = self.isa_graph.mat_node_by_name[start_name]
        else:
            dfs_start = None
        visitor.on_begin_assay(self.investigation, self.study, self.assay)
        # Visit all ISA nodes in DFS fashion.  For each node, register it in visitor, and yield
        # information to caller.
        for _node_id, obj_type, obj, node_path in self.isa_graph.dfs(dfs_start):
            new_obj = obj
            for func in func_mapping[obj_type]:
                tmp_obj = func(obj, node_path=node_path, study=self.study, assay=self.assay)
                new_obj = tmp_obj or new_obj
            if obj_type == TYPE_MATERIAL:
                self._materials[obj.unique_name] = new_obj
            elif obj_type == TYPE_PROCESS:
                self._processes[obj.unique_name] = new_obj
            yield "assay", self.assay, obj_type, obj
        visitor.on_end_assay(self.investigation, self.study, self.assay)
        logger.debug("end assay traversal %s", self.assay.file)

    def run(self, visitor: IsaNodeVisitor, start_name=None):
        return tuple(self.gen(visitor, start_name))

    def build_evolved_assay(self) -> Assay:
        """Return assay with updated materials and processes."""
        return attr.evolve(self.assay, materials=self._materials, processes=self._processes)


def first_value(key, node_path, default=None, ignore_case=True):
    for node in node_path:
        for attr_type in ("characteristics", "parameter_values"):
            for x in getattr(node, attr_type, ()):
                if (ignore_case and x.name.lower() == key.lower()) or (
                    not ignore_case and x.name == key
                ):
                    return ";".join(x.value)
    return default
