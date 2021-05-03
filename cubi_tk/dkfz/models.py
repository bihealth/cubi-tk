"""Models used for representing ISA-tab data structures (to play nice with DKFZ meta files only)."""

import copy
import re
import sys
import typing

import pandas as pd
from logzero import logger

import pdb


class MultiColumns:
    columns = []

    @classmethod
    def _get_columns(cls):
        return cls.columns

    def __init__(self):
        self.values = {x: [] for x in self.columns}
        self.defaults = {x: None for x in self.columns}
        self.active = set()
        self.size = 0

    def __str__(self):
        ", ".join(
            self.columns
        )  # ["{}: {}".format(k, v if k in self.active else "Inactive") for (k, v) in self.values.items()])

    def __len__(self):
        return self.size

    @staticmethod
    def _get_default(x):
        if (x is None) or (len(x) == 0):
            return None
        unique = list(set(x).difference(set([None])))
        if len(unique) != 1:
            return None
        return unique[0]

    def set_size(self, N):
        if self.size == N:
            return
        if self.size > 0 and N > 0 and self.size != N:
            raise ValueError("Can't reset size from {} to {}".format(self.size, N))
        for category in self.active:
            self.values[category] = [self.defaults[category]] * N
        self.size = N

    def fill_with_defaults(self, N):
        my_copy = copy.deepcopy(self)
        my_copy.set_size(0)
        for category in my_copy.active:
            my_copy.values[category] = [my_copy.defaults[category]] * N
        my_copy.size = N
        return my_copy

    def set_default(self, value=None, category=None):
        if (category is None) or (not category in self.columns):
            raise ValueError(
                "Missing or illegal category {} to set default value".format(category)
            )
        self.active = self.active.union([category])
        self.defaults[category] = value

    def set_values(self, values=[], category=None, update_default=True):
        if (category is None) or (not category in self.columns):
            raise ValueError(
                "Missing or illegal category {} to set values".format(category)
            )
        if (self.size > 0) and (len(values) > 0) and (self.size != len(values)):
            raise ValueError(
                "Cannot set values for category {}, previous size = {}, target size = {}".format(
                    category, self.size, len(values)
                )
            )
        self.active.union([category])
        if (not values is None) and (len(values) > 0):
            self.values[category] = values
            self.size = len(values)
        if update_default and (not self.defaults[category]):
            self.defaults[category] = MultiColumns._get_default(values)

    def _extend(
        self, other: "MultiColumns", allow_different_columns=False, update_defaults=True
    ):
        x = self.active
        y = other.active
        common = list(x.intersection(y))
        xonly = list(x.difference(y))
        yonly = list(y.difference(x))
        if (not allow_different_columns) and ((len(xonly) > 0) or (len(yonly) > 0)):
            raise ValueError("Can't append objects with different columns")
        for category in common:
            if update_defaults and (not self.defaults[category]):
                if other.defaults[category]:
                    self.defaults[category] = other.defaults[category]
                else:
                    self.defaults[category] = MultiColumns._get_default(
                        other.values[category]
                    )
            if (self.size > 0) and (len(self.values[category]) == 0):
                self.values[category] = [self.defaults[category]] * self.size
            self.values[category].extend(other.values[category])
        for category in xonly:
            self.values[category].extend([self.defaults[category]] * other.size)
        for category in yonly:
            my_copy = copy.deepcopy(other.values[category])
            my_copy.insert(0, [other.defaults[category]] * self.size)
            self.values[category] = my_copy
            self.active.union([category])
        self.size += other.size


class Annotation(MultiColumns):
    pattern = "{}"
    columns = ["values", "unit", "ref", "accession"]

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.active = set(["values"])

    def __str__(self):
        return "name: {}, size: {}, {}".format(
            self.name, self.size, self.values["values"]
        )  # super().__str__()))

    def extend(self, annotation: "Annotation", update_defaults=True):
        if self.name != annotation.name:
            raise ValueError(
                "Cannot extend two annotations with different names ({} and {})".format(
                    self.name, annotation.name
                )
            )
        super()._extend(annotation, update_defaults=update_defaults)

    def get_DataFrame(self):
        df = {}
        for (k, v) in {
            "values": self.pattern.format(self.name),
            "unit": "Unit",
            "ref": "Term Source REF",
            "accession": "Term Accession Number",
        }.items():
            if k in self.active:
                x = self.values[k]
                if not x:
                    x = [self.defaults[k]] * self.size
                df[v] = x
        return pd.DataFrame(data=df)


class Characteristics(Annotation):
    pattern = "Characteristics[{}]"

    def __init(self, name):
        super().__init__(name)


class Parameter(Annotation):
    pattern = "Parameter[{}]"

    def __init(self, name):
        super().__init__(name)


class Comment(Annotation):
    pattern = "Comment[{}]"

    def __init(self, name):
        super().__init__(name)


class Node(MultiColumns):
    pattern = "{}"

    def __init__(self, name):
        super().__init__()
        self.name = name
        self.annotations = []
        self.comments = []

    def __str__(self):
        return (
            "name: {}, size: {}, {}".format(
                self.name,
                self.size,
                self.values["values"] if "values" in self.values.keys() else "",
            )
            + "[("
            + "), (".join(x.__str__() for x in self.annotations)
            + ")]"
            if self.annotations
            else "None" + "[(" + "), (".join(x.__str__() for x in self.comments) + ")]"
            if self.comments
            else "None"
        )

    def _extend(self, node: "Node", update_defaults=True):
        if self.name != node.name:
            raise ValueError(
                "Cannot extend two nodes with different names ({} and {})".format(
                    self.name, node.name
                )
            )

        x = set([a.name for a in self.annotations])
        y = set([a.name for a in node.annotations])
        yonly = y.difference(x)
        for annotation in self.annotations:
            found = False
            for a in node.annotations:
                if annotation.name == a.name:
                    # print("DEBUG- extending common annotation {} in {}, initial size = {}, extension size = {}".format(annotation.name, self.name, annotation.size, a.size))
                    annotation.extend(a)
                    # print("DEBUG- annotation extension OK, current size = {}".format(annotation.size))
                    found = True
                    break
            if not found:
                # print("DEBUG- padding annotation {} in {}, initial size = {}, extension size = {}".format(annotation.name, self.name, self.size, a.size))
                annotation.extend(annotation.fill_with_defaults(a.size))
                # print("DEBUG- annotation extension OK, current size = {}".format(annotation.size))
        for a in node.annotations:
            if a.name in yonly:
                # print("DEBUG- creating annotation {} in {}, initial size = {}, extension size = {}".format(a.name, self.name, self.size, a.size))
                tmp = a.fill_with_defaults(self.size)
                tmp.extend(a)
                self.annotations.append(tmp)
                # print("DEBUG- annotation extension OK, current size = {}".format("?"))

        x = set([a.name for a in self.comments])
        y = set([a.name for a in node.comments])
        yonly = y.difference(x)
        for comment in self.comments:
            found = False
            for a in node.comments:
                if comment.name == a.name:
                    # print("DEBUG- extending common comment {} in {}, initial size = {}, extension size = {}".format(comment.name, self.name, comment.size, a.size))
                    comment.extend(a)
                    # print("DEBUG- comment extension OK, current size = {}".format(comment.size))
                    found = True
                    break
            if not found:
                # print("DEBUG- padding comment {} in {}, initial size = {}, extension size = {}".format(comment.name, self.name, self.size, a.size))
                comment.extend(comment.fill_with_defaults(a.size))
                # print("DEBUG- comment extension OK, current size = {}".format(comment.size))
        for a in node.comments:
            if a.name in yonly:
                # print("DEBUG- creating comment {} in {}, initial size = {}, extension size = {}".format(a.name, self.name, self.size, a.size))
                tmp = a.fill_with_defaults(self.size)
                tmp.extend(a)
                self.comments.append(tmp)
                # print("DEBUG- comment extension OK, current size = {}".format("?"))

        # print("DEBUG- extending {}, initial size = {}, extension size = {}".format(self.name, self.size, node.size))
        super()._extend(node, update_defaults=update_defaults)
        # print("DEBUG- extension of main values OK, current size = {}".format(self.size))

    def _set_annotation(self, annotation: Annotation, comment=False):
        if self.size > 0 and annotation.size > 0 and self.size != annotation.size:
            raise ValueError(
                "Cannot set annotation {} for node {}, previous values size = {}, replacement values size = {}".format(
                    annotation.name, self.name, self.size, annotation.size
                )
            )
        if annotation.size == 0 and self.size > 0:
            annotation.set_size(self.size)
        if annotation.size > 0 and self.size == 0:
            self.set_size(annotation.size)
        if comment:
            self.comments = [c for c in self.comments if c.name != annotation.name]
            self.comments.append(annotation)
        else:
            self.annotations = [
                a for a in self.annotations if a.name != annotation.name
            ]
            self.annotations.append(annotation)

    def set_size(self, N):
        super().set_size(N)
        for annotation in self.annotations:
            annotation.set_size(N)
        for comment in self.comments:
            comment.set_size(N)

    def set_comment(self, comment: Comment):
        self._set_annotation(comment, comment=True)

    def get_DataFrame(self) -> pd.DataFrame:
        df = None
        for annotation in self.annotations:
            df = pd.concat([df, annotation.get_DataFrame()], axis=1)
        for comment in self.comments:
            df = pd.concat([df, comment.get_DataFrame()], axis=1)
        return df


class Material(Node):
    pattern = "{} Name"
    columns = ["name"]

    def __init__(self, name, values=[]):
        super().__init__(name)
        self.active = set(Material.columns)

    def set_values(self, values):
        super().set_values(values, category="name")

    def set_characteristic(self, characteristic: Characteristics):
        super()._set_annotation(characteristic)

    def extend(self, node: "Material", update_defaults=True):
        super()._extend(node, update_defaults=update_defaults)

    def get_DataFrame(self) -> pd.DataFrame:
        return pd.concat(
            [
                pd.DataFrame(
                    data={self.pattern.format(self.name): self.values["name"]}
                ),
                super().get_DataFrame(),
            ],
            axis=1,
        )


class Protocol(Node):
    pattern = "{}"
    columns = ["Performer", "Date"]

    def __init__(self, name):
        super().__init__(name)
        self.active = set(Protocol.columns)

    def set_parameter(self, parameter: Parameter):
        super()._set_annotation(parameter)

    def extend(self, node: "Protocol", update_defaults=True):
        super()._extend(node, update_defaults=update_defaults)

    def get_DataFrame(self) -> pd.DataFrame:
        return pd.concat(
            [
                pd.DataFrame(data={"Protocol REF": [self.name] * self.size}),
                super().get_DataFrame(),
                pd.DataFrame(data={"Performer": self.values["Performer"]}),
                pd.DataFrame(data={"Date": self.values["Date"]}),
            ],
            axis=1,
        )


class Assay:
    def __init__(self, assay_type):
        self.assay_type = assay_type
        self.Materials = {}
        self.Protocols = {}

    def __str__(self):
        sep = "-" * 60 + "\n"
        SEP = "=" * 60 + "\n"
        return (
            SEP
            + sep.join(
                ["Material\n{}\n".format(x.__str__()) for x in self.Materials.values()]
            )
            + SEP
            + sep.join(
                ["Protocol\n{}\n".format(x.__str__()) for x in self.Protocols.values()]
            )
            + SEP
        )

    def set_size(self, N):
        for material in self.Materials.values():
            material.set_size(N)
        for protocol in self.Protocols.values():
            protocol.set_size(N)

    def set_material(self, material: Material):
        self.Materials[material.name] = material

    def set_protocol(self, protocol: Protocol):
        self.Protocols[protocol.name] = protocol

    def extend(self, assay: "Assay"):
        if self.assay_type != assay.assay_type:
            raise ValueError(
                "Cannot extend assay of type {} by assay of type {}".format(
                    self.assay_type, assay.assay_type
                )
            )
        if set(self.Materials.keys()) != set(assay.Materials.keys()):
            raise ValueError(
                "Cannot extend assay {}, different material nodes".format(
                    self.assay_type
                )
            )
        if set(self.Protocols.keys()) != set(assay.Protocols.keys()):
            raise ValueError(
                "Cannot extend assay {}, different protocol nodes".format(
                    self.assay_type
                )
            )
        for (name, material) in self.Materials.items():
            material.extend(assay.Materials[name])
        for (name, protocol) in self.Protocols.items():
            protocol.extend(assay.Protocols[name])
