from datetime import datetime
import re
import sys

import pandas as pd

from logzero import logger

from .models import Assay
from .models import Material
from .models import Protocol
from .models import Characteristics
from .models import Parameter
from .models import Comment

import pdb


class DkfzMeta:

    _token = object()
    pattern = re.compile(
        "^([A-Z0-9]+-)?(([A-Z0-9]{4,6})-(([BFTM])([0-9]+)))-(([DR])([0-9]+))(-[0-9]+)?$"
    )
    cubi_naming = {
        "extract": {"exome": "DNA", "transcriptome": "RNA", "whole genome": "DNA"},
        "library": {"exome": "WES", "transcriptome": "mRNA_seq", "whole genome": "WGS"},
    }

    def __init__(self, _from_factory=None):
        if _from_factory is not DkfzMeta._token:
            raise NotImplementedError("Initialisation only from parser")
        self.config = {}
        self.meta_filename = []
        self.meta = []
        self.assays = {}

    def create_cubi_names(self, mapping=None):
        for (sequencing_type, assay) in self.assays.items():
            extract = ""
            library = ""
            if sequencing_type == "exome":
                extract = "DNA1"
                library = "WES1"
            elif sequencing_type == "whole genome":
                extract = "DNA1"
                library = "WGS1"
            elif sequencing_type == "transcriptome":
                extract = "RNA1"
                library = "mRNA_seq1"
            else:
                continue

            rename_extract = True
            rename_library = True
            if not mapping is None:
                df = pd.DataFrame(
                    data={
                        "Sample Name": assay["isatab"]
                        .Materials["Sample"]
                        .values["name"]
                    }
                )
                df = df.merge(
                    mapping, on="Sample Name", how="left", validate="many_to_one"
                )
                assay["isatab"].Materials["Sample"].set_values(
                    df["Sample Name CUBI"].tolist()
                )
                if "Source Name CUBI" in df.columns:
                    assay["isatab"].Materials["Source"].set_values(
                        df["Source Name CUBI"].tolist()
                    )
                if "Extract Name CUBI" in df.columns:
                    assay["isatab"].Materials["Extract"].set_values(
                        df["Extract Name CUBI"].tolist()
                    )
                    rename_extract = False
                if "Library Name CUBI" in df.columns:
                    assay["isatab"].Materials["Library"].set_values(
                        df["Library Name CUBI"].tolist()
                    )
                    rename_library = False
                for x in df.columns:
                    if x in [
                        "Source Name",
                        "Source Name CUBI",
                        "Sample Name",
                        "Sample Name CUBI",
                        "Extract Name CUBI",
                        "Library Name CUBI",
                    ]:
                        continue
                    characteristic = Characteristics(x)
                    characteristic.set_values(df[x].to_list(), category="values")
                    assay["isatab"].Materials["Sample"].set_characteristic(
                        characteristic
                    )

            if rename_extract:
                assay["isatab"].Materials["Extract"].set_values(
                    [
                        x + "-" + extract
                        for x in assay["isatab"].Materials["Sample"].values["name"]
                    ]
                )
            if rename_library:
                assay["isatab"].Materials["Library"].set_values(
                    [
                        x + "-" + library
                        for x in assay["isatab"].Materials["Extract"].values["name"]
                    ]
                )

    def get_assay(self, assay_type, drop_assay=True):
        if not assay_type in self.assays:
            return None
        assay = self.assays[assay_type]["isatab"]

        df = pd.concat(
            [
                pd.DataFrame(
                    data={"Sample Name": assay.Materials["Sample"].values["name"]}
                ),
                assay.Protocols["Nucleic acid extraction"].get_DataFrame(),
                assay.Materials["Extract"].get_DataFrame(),
                assay.Protocols["Library construction"].get_DataFrame(),
                assay.Materials["Library"].get_DataFrame(),
                assay.Protocols["Nucleic acid sequencing"].get_DataFrame(),
                assay.Materials["Assay"].get_DataFrame(),
            ],
            axis=1,
        )

        if drop_assay:
            try:
                i = df.columns.tolist().index("Assay Name")
                df = df.iloc[range(df.shape[0]), range(i)]
            except ValueError:
                pass

        return df.drop_duplicates()

    def get_sample(self):
        dfs = None
        for assay_type in self.assays.keys():
            assay = self.assays[assay_type]["isatab"]

            df = pd.concat(
                [
                    assay.Materials["Source"].get_DataFrame(),
                    assay.Protocols["Sample collection"].get_DataFrame(),
                    assay.Materials["Sample"].get_DataFrame(),
                ],
                axis=1,
            )

            if not dfs is None:
                dfs = pd.concat(
                    [dfs, df.reset_index(drop=True).drop_duplicates()],
                    axis=0,
                    ignore_index=True,
                )
            else:
                dfs = df

        return dfs.drop_duplicates()

    def extend(self, meta):
        self.meta_filename.extend(meta.meta_filename)
        self.meta.extend(meta.meta)
        for (assay_type, assay) in meta.assays.items():
            if assay_type in self.assays:
                self.assays[assay_type]["meta"] = self.assays[assay_type][
                    "meta"
                ].append(assay["meta"])
                for (k, v) in assay["Internals"].items():
                    if not k in self.assays[assay_type]["Internals"]:
                        self.assays[assay_type]["Internals"][k] = [None] * self.assays[
                            assay_type
                        ]["isatab"].Materials["Sample"].size
                    self.assays[assay_type]["Internals"][k].extend(v)
                for (k, v) in self.assays[assay_type]["Internals"].items():
                    if not k in assay["Internals"]:
                        self.assays[assay_type]["Internals"][k].extend(
                            [None] * assay["isatab"].Materials["Sample"].size
                        )
                self.assays[assay_type]["isatab"].extend(assay["isatab"])
            else:
                self.assays[assay_type] = assay

    @staticmethod
    def find_sample_nb(df, original="sample_nb", cubi="CUBI_sample_nb"):
        x = sorted(list(set(df[original].astype(str).tolist())))
        x = dict(zip([str(y) for y in x], range(len(x))))
        df.loc[:, cubi] = [x[str(y)] + 1 for y in df[original].astype(str).tolist()]
        return df

    def dktk(self):
        dfs = None
        for assay_type in self.assays.keys():
            assay = self.assays[assay_type]["isatab"]

            provider_id = Characteristics("Provider sample id")
            provider_id.set_values(
                assay.Materials["Sample"].values["name"], category="values"
            )
            assay.Materials["Sample"].annotations.append(provider_id)

            md5 = None
            for annotation in assay.Materials["Assay"].annotations:
                if annotation.name == "Checksum":
                    md5 = annotation.values["values"]
                    break

            df = pd.DataFrame(
                data={
                    "Sample Name": assay.Materials["Sample"].values["name"],
                    "Checksum": md5,
                }
            )
            df = df.merge(
                pd.DataFrame(data=self.assays[assay_type]["Internals"]),
                how="left",
                on="Checksum",
            )

            is_tumor = None
            if ("Tissue_type" in df.columns) and (
                set(df["Tissue_type"].tolist()).issubset({"N", "T"})
            ):
                is_tumor = Characteristics("Is Tumor")
                is_tumor.set_values(df["Tissue_type"].tolist(), category="values")
                assay.Materials["Sample"].annotations.append(is_tumor)

                is_metastasis = Characteristics("Is Metastasis")
                v = assay.Materials["Sample"].values["name"]
                p = DkfzMeta.pattern
                is_metastasis.set_values(
                    [p.match(x).group(5) if p.match else "" for x in v],
                    category="values",
                )
                verify = list(
                    zip(is_tumor.values["values"], is_metastasis.values["values"])
                )
                if not all(
                    [
                        (x[0] == "N" and x[1] != "T" and x[1] != "M")
                        or (x[0] == "T" and (x[1] == "T" or x[1] == "M"))
                        for x in verify
                    ]
                ):
                    logger.warning(
                        "Can't infer metastasis status for assay {}".format(assay_type)
                    )
                else:
                    is_metastasis.set_values(
                        [x == "M" for x in is_metastasis.values["values"]],
                        category="values",
                    )
                    assay.Materials["Sample"].annotations.append(is_metastasis)
            else:
                logger.warning(
                    "Can't infer normal/tumor status for assay {}".format(assay_type)
                )

            batch = None
            if "Batch" in df.columns:
                batch = Characteristics("Batch")
                batch.set_values(df["Batch"].astype(int).tolist(), category="values")
                assay.Materials["Library"].annotations.append(batch)
            else:
                logger.warning("Can't set batch number for assay {}".format(assay_type))

            if is_tumor and batch:
                dfs = pd.concat(
                    [
                        dfs,
                        pd.DataFrame(
                            data={
                                "Source Name": assay.Materials["Source"].values["name"],
                                "Sample Name": assay.Materials["Sample"].values["name"],
                                "Provider ID": provider_id.values["values"],
                                "Is Tumor": is_tumor.values["values"],
                                "Batch": batch.values["values"],
                                "assay_type": [assay_type]
                                * assay.Materials["Sample"].size,
                            }
                        ),
                    ],
                    axis=0,
                    ignore_index=True,
                )

        if not dfs is None:
            dfs = dfs.reset_index(drop=True).drop_duplicates()

            p = DkfzMeta.pattern
            dfs["sample_id"] = [
                p.match(x).group(2) if p.match(x) else ""
                for x in dfs["Provider ID"].tolist()
            ]
            dfs = dfs.sort_values(["Source Name", "assay_type", "Batch"])

            cols = ["Source Name", "Is Tumor"]
            orig = "sample_id"
            cubi = "cubi_sample_nb"
            dfs = (
                dfs.groupby(cols)
                .apply(lambda x: DkfzMeta.find_sample_nb(x, original=orig, cubi=cubi))
                .reset_index(drop=True)
            )

            cols = ["sample_id", "assay_type"]
            orig = "Provider ID"
            cubi = "cubi_extract_nb"
            dfs = (
                dfs.groupby(cols)
                .apply(lambda x: DkfzMeta.find_sample_nb(x, original=orig, cubi=cubi))
                .reset_index(drop=True)
            )

            cols = ["sample_id", "assay_type", "cubi_extract_nb"]
            orig = "Batch"
            cubi = "cubi_library_nb"
            dfs = (
                dfs.groupby(cols)
                .apply(lambda x: DkfzMeta.find_sample_nb(x, original=orig, cubi=cubi))
                .reset_index(drop=True)
            )

            dfs["Sample Name CUBI"] = (
                dfs["Source Name"]
                + "-"
                + dfs["Is Tumor"]
                + dfs["cubi_sample_nb"].astype(str)
            )
            dfs["Extract Name CUBI"] = (
                dfs["Sample Name CUBI"]
                + "-"
                + [
                    DkfzMeta.cubi_naming["extract"][x]
                    for x in dfs["assay_type"].tolist()
                ]
                + dfs["cubi_extract_nb"].astype(str)
            )
            dfs["Library Name CUBI"] = (
                dfs["Extract Name CUBI"]
                + "-"
                + [
                    DkfzMeta.cubi_naming["library"][x]
                    for x in dfs["assay_type"].tolist()
                ]
                + dfs["cubi_library_nb"].astype(str)
            )

            tmp = dfs.groupby("Sample Name CUBI").filter(
                lambda x: any([y > 1 for y in x["cubi_library_nb"].tolist()])
                or any([y > 1 for y in x["cubi_extract_nb"].tolist()])
            )
            tmp = (
                tmp.groupby("Sample Name CUBI")
                .aggregate(
                    {
                        "Library Name CUBI": lambda x: ", ".join(set(x)),
                        "Batch": lambda x: ", ".join(set(x.astype(str))),
                    }
                )
                .reset_index()
            )
            for i in range(tmp.shape[0]):
                logger.warning(
                    "Sample {} has snappy-incompatible libraries {} from batches {}".format(
                        tmp.iloc[i, 0], tmp.iloc[i, 1], tmp.iloc[i, 2]
                    )
                )

            dfs = dfs[
                [
                    "Sample Name",
                    "Sample Name CUBI",
                    "Extract Name CUBI",
                    "Library Name CUBI",
                ]
            ].drop_duplicates()

            if any(dfs["Sample Name"].duplicated()) or any(
                dfs["Library Name CUBI"].duplicated()
            ):
                logger.error(
                    "Creation of CUBI sample ids is not possible, mapping not 1-1"
                )
                return None

        return dfs

    def filename_mapping(
        self, assay_type, sodar_path=None, date=str(datetime.date(datetime.now()))
    ):
        assay = self.assays[assay_type]
        pattern = re.compile("^.*/?(AS-[0-9]+-LR-[0-9]+)_R([12])\\.fastq\\.gz$")

        df = pd.DataFrame(
            data={
                "source_path": assay["Internals"]["fastq_path"],
                "folder_name": assay["isatab"].Materials["Library"].values["name"],
                "mate": assay["Internals"]["Mate"],
                "checksum": assay["Internals"]["Checksum"],
            }
        )

        df["basename"] = [
            pattern.match(x).group(1) if pattern.match else None
            for x in df["source_path"].tolist()
        ]

        df = (
            df.groupby("folder_name")
            .apply(
                lambda x: DkfzMeta.find_sample_nb(
                    x, original="basename", cubi="library_nb"
                )
            )
            .reset_index(drop=True)
        )
        library_name = [
            "%s_%03d_R%d.fastq.gz" % (x[0], x[1], x[2])
            for x in list(
                zip(
                    df["folder_name"].tolist(),
                    df["library_nb"].tolist(),
                    df["mate"].tolist(),
                )
            )
        ]
        df["library_name"] = library_name

        return df
