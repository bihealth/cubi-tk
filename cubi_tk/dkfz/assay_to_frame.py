import pandas as pd

import altamisa.isatab.models

from .DkfzExceptions import MissingValueError, DuplicateValueError, IllegalValueError


class AssayToFrame:
    def __init__(self, assay):
        self.assay = assay

        self.parent = {}
        self.heads = set()
        self.tails = set()
        self.title = []
        self.path = []
        self.attributes = {}

    def get_data_frame(self):
        self._get_parents()

        # Prepare the title row & ensure same column order for all rows
        current = list(self.tails)[0]
        while current:
            if current in self.assay.materials.keys():
                self._add_material_columns(current)
            elif current in self.assay.processes.keys():
                self._add_process_columns(current)
            else:
                raise IllegalValueError("Unknown element {}".format(current))
            current = self.parent[current] if current in self.parent.keys() else None

        df = list()
        for raw_data_file in list(self.tails):
            row = self._assay_row(raw_data_file)
            df.append(row)

        return pd.DataFrame(columns=self.title, data=df)

    def _get_parents(self):
        hasChild = set()
        hasParent = set()
        for a in list(self.assay.arcs):
            hasChild.add(a.head)
            hasParent.add(a.tail)
            self.parent[a.tail] = a.head
        self.heads = hasChild - hasParent
        self.tails = hasParent - hasChild

    def _add_material_columns(self, current):
        m = self.assay.materials[current]
        currentType = m.type
        self.path.append(currentType)
        if currentType in self.attributes.keys():
            raise DuplicateValueError(
                "Assay graph not DAG: type {} appears multiple times in path".format(currentType)
            )
        self.attributes[currentType] = {
            "characteristics": [(c.name, c.value[0].__class__) for c in m.characteristics],
            "factors": [v.name for v in m.factor_values],
            "comments": [c.name for c in m.comments],
        }
        for v in self.attributes[currentType]["comments"]:
            self.title.insert(0, "Comment[{}]".format(v))
        for v in self.attributes[currentType]["factors"]:
            self.title.insert(0, "Factor Value[{}]".format(v))
        for v in self.attributes[currentType]["characteristics"]:
            if v[1] != "str".__class__:
                self.title.insert(0, "Term Accession Number")
                self.title.insert(0, "Term Source REF")
            self.title.insert(0, "Characteristics[{}]".format(v[0]))
        self.title.insert(0, currentType)

    def _add_process_columns(self, current):
        p = self.assay.processes[current]
        currentType = p.protocol_ref
        self.path.append(currentType)
        if currentType in self.attributes.keys():
            raise DuplicateValueError(
                "Assay graph not DAG: type {} appears multiple times in path".format(currentType)
            )
        self.attributes[currentType] = {
            "parameters": [(v.name, v.value[0].__class__) for v in p.parameter_values],
            "comments": [c.name for c in p.comments],
        }
        self.title.insert(0, "Date")
        self.title.insert(0, "Performer")
        for v in self.attributes[currentType]["comments"]:
            self.title.insert(0, "Comment[{}]".format(v))
        for v in self.attributes[currentType]["parameters"]:
            if v[1] != "str".__class__:
                self.title.insert(0, "Term Accession Number")
                self.title.insert(0, "Term Source REF")
            self.title.insert(0, "Parameter Value[{}]".format(v[0]))
        self.title.insert(0, "Protocol REF")

    def _assay_row(self, raw_data_file):
        materials = self.assay.materials
        processes = self.assay.processes
        row = []
        i = 0
        current = raw_data_file
        while current:
            currentType = None
            if current in materials.keys():
                m = materials[current]
                currentType = m.type
                AssayToFrame._insert_at_start(
                    row,
                    AssayToFrame._add_comments(
                        self.attributes[currentType]["comments"], m, raw_data_file
                    ),
                )
                AssayToFrame._insert_at_start(
                    row,
                    AssayToFrame._add_factors(
                        self.attributes[currentType]["factors"], m, raw_data_file
                    ),
                )
                AssayToFrame._insert_at_start(
                    row,
                    AssayToFrame._add_characteristics(
                        self.attributes[currentType]["characteristics"], m, raw_data_file
                    ),
                )
                row.insert(0, m.name)

            elif current in processes.keys():
                p = processes[current]
                currentType = p.protocol_ref
                row.insert(0, p.date)
                row.insert(0, p.performer)
                AssayToFrame._insert_at_start(
                    row,
                    AssayToFrame._add_comments(
                        self.attributes[currentType]["comments"], p, raw_data_file
                    ),
                )
                AssayToFrame._insert_at_start(
                    row,
                    AssayToFrame._add_parameters(
                        self.attributes[currentType]["parameters"], p, raw_data_file
                    ),
                )
                row.insert(0, currentType)

            else:
                raise IllegalValueError("Unknown element {}".format(current))

            if currentType is None or i >= len(self.path) or currentType != self.path[i]:
                print(
                    "Incompatible paths, {} not added to assay ISATAB table".format(raw_data_file)
                )
                continue
            current = self.parent[current] if current in self.parent.keys() else None
            i += 1

        return row

    @staticmethod
    def _add_comments(path, materialOrProcess, raw_data_file):
        values = []
        for element in path:
            found = False
            for c in materialOrProcess.comments:
                if c.name == element:
                    found = True
                    values.insert(0, c.value)
                    break
            if not found:
                raise MissingValueError("No comment {} in {}".format(element, raw_data_file))
        return values

    @staticmethod
    def _add_factors(path, material, raw_data_file):
        values = []
        for element in path:
            found = False
            for v in material.factor_values:
                if v.name == element:
                    found = True
                    values.insert(0, v.value)
                    break
            if not found:
                raise MissingValueError("No factor value {} in {}".format(element, raw_data_file))
        return values

    @staticmethod
    def _add_characteristics(path, material, raw_data_file):
        values = []
        for element in path:
            found = False
            for c in material.characteristics:
                if (c.name == element[0]) and (c.value[0].__class__ == element[1]):
                    found = True
                    if isinstance(c.value[0], altamisa.isatab.models.OntologyTermRef):
                        values.insert(0, ";".join([x.accession for x in c.value]))
                        values.insert(0, ";".join([x.ontology_name for x in c.value]))
                        values.insert(0, ";".join([x.name for x in c.value]))
                    else:
                        values.insert(0, ";".join(c.value))
                    break
            if not found:
                raise MissingValueError("No characteristic {} in {}".format(element, raw_data_file))
        return values

    @staticmethod
    def _add_parameters(path, process, raw_data_file):
        values = []
        for element in path:
            found = False
            for v in process.parameter_values:
                if (v.name == element[0]) and (v.value[0].__class__ == element[1]):
                    found = True
                    if isinstance(v.value[0], altamisa.isatab.models.OntologyTermRef):
                        values.insert(0, ";".join([x.accession for x in v.value]))
                        values.insert(0, ";".join([x.ontology_name for x in v.value]))
                        values.insert(0, ";".join([x.name for x in v.value]))
                    else:
                        values.insert(0, ";".join(v.value))
                    break
            if not found:
                raise MissingValueError("No parameter {} in {}".format(element, raw_data_file))
        return values

    @staticmethod
    def _insert_at_start(theList, inserted):
        if not inserted:
            return
        inserted.reverse()
        for x in inserted:
            theList.insert(0, x)
