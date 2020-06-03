"""Helper code for working with altamisa objects."""

from itertools import chain
import typing

from logzero import logger

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
        logger.debug("begin study %s", study.file)

    def on_end_study(self, investigation, study):
        logger.debug("end study %s", study.file)

    def on_begin_assay(self, investigation, study, assay):
        logger.debug("begin assay %s", assay.file)

    def on_end_assay(self, investigation, study, assay):
        logger.debug("end assay %s", assay.file)

    def on_traverse_arc(self, arc, node_path, study=None, assay=None):
        logger.debug("traversing arc %s", arc)

    def on_visit_node(self, node, node_path, study=None, assay=None):
        logger.debug("visiting node %s", node)

    def on_visit_material(self, material, node_path, study=None, assay=None):
        logger.debug("visiting material %s", material)
        self.on_visit_node(material, study, assay)

    def on_visit_process(self, process, node_path, study=None, assay=None):
        logger.debug("visiting process %s", process)
        self.on_visit_node(process, study, assay)


class InvestigationTraversal:
    """Allow for easy traversal of an investigation."""

    def __init__(self, investigation, studies, assays):
        #: Investigation object.
        self.investigation = investigation
        #: Mapping from study name (file name) to Study
        self.studies = dict(studies)
        #: Mapping from assay name (file name) to Assay
        self.assays = assays

    def gen(self, visitor: IsaNodeVisitor):
        logger.debug("start investigation traversal")
        visitor.on_begin_investigation(self.investigation)
        for file_name, study in self.studies.items():
            logger.debug("create study traversal %s", file_name)
            st = StudyTraversal(self, study, self.assays)
            yield from st.gen(visitor)
            logger.debug("finalize study traversal %s", file_name)
        visitor.on_end_investigation(self.investigation)
        logger.debug("end investigation traversal")

    def run(self, visitor: IsaNodeVisitor):
        return tuple(self.gen(visitor))


class StudyTraversal:
    """Allow for easy traversal of a study."""

    def __init__(self, investigation, study, assays):
        self.investigation = investigation
        self.study = study
        self.assays = assays
        self.isa_graph = IsaGraph(self.study.materials, self.study.processes, self.study.arcs)
        self.assay_traversals = [
            AssayTraversal(investigation, study, assay) for assay in assays.values()
        ]

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
            for func in func_mapping[obj_type]:
                func(obj, node_path=node_path, study=self.study)
            yield "study", self.study, obj_type, obj
            if node_id in self.isa_graph.ends:
                assert obj_type == TYPE_MATERIAL
                for assay_traversal in self.assay_traversals:
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


class AssayTraversal:
    """Allow for easy traversal of assay."""

    def __init__(self, investigation, study, assay):
        self.investigation = investigation
        self.study = study
        self.assay = assay
        self.isa_graph = IsaGraph(self.assay.materials, self.assay.processes, self.assay.arcs)

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
        for node_id, obj_type, obj, node_path in self.isa_graph.dfs(dfs_start):
            for func in func_mapping[obj_type]:
                func(obj, node_path=node_path, study=self.study, assay=self.assay)
            yield "assay", self.assay, obj_type, obj
        visitor.on_end_assay(self.investigation, self.study, self.assay)
        logger.debug("end assay traversal %s", self.assay.file)

    def run(self, visitor: IsaNodeVisitor, start_name=None):
        return tuple(self.gen(visitor, start_name))
