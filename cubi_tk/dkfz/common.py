"""``cubi-tk dkfz ingest-fastq``: transfer raw FASTQs into iRODS landing zone."""

import argparse
import attr
import os
import textwrap
import typing
import yaml

from pathlib import Path
from typing import List, Dict, Tuple, Any

from logzero import logger

import altamisa.isatab.models

from .DkfzMetaParser import DkfzMetaParser
from .IdMapper import IdMapper
from .DkfzMeta import DkfzMeta

@attr.s(frozen=True, auto_attribs=True)
class Config:
    """Configuration for the pull-raw-data."""

    verbose: bool
    sodar_server_url: str
    sodar_api_token: str = attr.ib(repr=lambda value: "***")  # type: ignore
    mapping_config: str
    parsing_config: str
    meta: List[str]

class DkfzCommandBase:
    """Implementation of dkfz prepare-isatab command for raw data."""

    command_name = ""
    step_name = "raw_data"

    def __init__(self, config: Config):
        self.config = config
        self.parser = None
        self.mapper = None

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="dkfz_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--mapping-config",
            dest="mapping_config",
            default=os.path.dirname(__file__) + "/../isa_tpl/isatab-dkfz/DkfzMetaIdMappings.yaml",
            help="Configuration file for the Dkfz id mapper"
        )

        parser.add_argument(
            "--parsing-config",
            dest="parsing_config",
            default=os.path.dirname(__file__) + "/../isa_tpl/isatab-dkfz/DkfzMetaParser.yaml",
            help="Configuration file for the Dkfz metafile parser"
        )

        parser.add_argument("meta", nargs="+", help="DKFZ meta file(s)")

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0
        return res

    def execute(self) -> typing.Optional[int]:
        raise NotImplementedError("Must be implemented in derived classes")

    def _get_parser(self) -> DkfzMetaParser:
        schema = None
        with open(self.config.parsing_config, "r") as f:
            schema = yaml.safe_load(f)
        logger.info("Created parser with configuration {}".format(self.config.parsing_config))
        self.parser = DkfzMetaParser(schema)

    def _get_id_mapper(self) -> IdMapper:
        schema = None
        with open(self.config.mapping_config, "r") as f:
            schema = yaml.safe_load(f)
        logger.info("Created ID mapper with configuration {}".format(self.config.mapping_config))
        self.mapper = IdMapper(schema)

    def read_metas(self) -> List[DkfzMeta]:
        self._get_parser()

        metas = []
        for filename in self.config.meta:
            meta = None
            with open(filename, "r") as f:
                meta = self.parser.read_meta(f)
                logger.info("Parsed metafile {}".format(filename))
            metas.append(meta)

        return metas

    def map_ids(self, metas: List[DkfzMeta]):
        self._get_id_mapper()

        if self.mapper.df is None:
            for meta in metas:
                self.mapper.aggregate_mappings(meta)
            self.mapper.df = self.mapper.mappings_table()

        for meta in metas:
            self.mapper.apply_mappings(meta)

    def _get_assay(self, metas: List[DkfzMeta], assay_type: str) -> altamisa.isatab.models.Assay:
        materials = {}
        processes = {}
        arcs      = set()
        for meta in metas:
            if assay_type in meta.content.keys():
                for md5, row in meta.content[assay_type].items():
                    if row.mapped is None:
                        raise MissingValueError("Unmapped row {}".format(md5))
                    for m in row.mapped.materials:
                        if not m.unique_name in materials.keys():
                            materials[m.unique_name] = m
                    for p in row.mapped.processes:
                        if not p.unique_name in processes.keys():
                            processes[p.unique_name] = p
                    for a in row.mapped.arcs:
                        arcs.add(a)
        return altamisa.isatab.models.Assay(
            file=None,
            header=None,
            materials=materials,
            processes=processes,
            arcs=tuple(list(arcs))
        )

    def get_assays(self) -> Dict[str, altamisa.isatab.models.Assay]:
        metas = self.read_metas()
        self.map_ids(metas)

        assay_types = set()
        for meta in metas:
            assay_types.update(list(meta.content.keys()))
        
        assays = {}
        for assay_type in list(assay_types):
            assays[assay_type] = self._get_assay(metas=metas, assay_type=assay_type)

        return assays

def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk dkfz``."""
    return DkfzCommandBase.setup_argparse(parser)
