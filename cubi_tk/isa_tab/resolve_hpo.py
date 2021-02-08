"""``cubi-tk isa-tab resolve-hpo``: resolve HPO terms to ISA-tab fragments."""

import argparse
import os
import re
import sys
import tempfile
import typing

import hpo_similarity.ontology
from logzero import logger
import requests
import termcolor
import tqdm

from ..exceptions import ResourceDownloadError

#: Template for creating HPO term from number.
TPL = "HP:{:07}"

#: Regular expression for parsing HPO out from text.
RE_HPO = r".*?HPO?:(\d+).*"

#: Default URL to get OBO file from.
URL_OBO = "http://purl.obolibrary.org/obo/hp.obo"


# Monkey-patch broken library...
def _add_hpo_attributes_to_node(graph, node_id, obo_tags):
    for key in obo_tags:
        if len(obo_tags[key]) > 1:
            graph.nodes[node_id][key] = [str(ot) for ot in obo_tags[key]]
        else:
            graph.nodes[node_id][key] = str(obo_tags[key][0])


hpo_similarity.ontology.add_hpo_attributes_to_node = _add_hpo_attributes_to_node


def missing(s):
    """Return ``s`` highlighted for missing text."""
    return termcolor.colored("!!MISSING(%s)!!" % s, "white", "on_red")


def escape(s):
    """Helper for escaping semicolon by urlencoding."""
    return s.replace(";", "%3B")


def fixup(s):
    """Fix HP notation"""
    if re.match(RE_HPO, s):  # HPO term
        m = re.match(RE_HPO, s)
        return TPL.format(int(m.group(1)))
    else:
        return s


def download_with_progress(url, out_path):
    """Helper to download a file with progress display."""
    r = requests.get(url, stream=True)
    total_size = int(r.headers.get("content-length", 0))
    block_size = 1024  # 1 Kibibyte
    with tqdm.tqdm(total=total_size, unit="iB", unit_scale=True) as t:
        with open(out_path, "wb") as f:
            for data in r.iter_content(block_size):
                t.update(len(data))
                f.write(data)


class ResolveHpoCommand:
    """Implementation of the ``resolve-hpo`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup argument parser."""
        parser.add_argument(
            "--hidden-cmd", dest="isa_tab_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument("--hpo-obo-url", default=URL_OBO, help="Default URL to OBO file.")

        parser.add_argument(
            "term_file",
            type=argparse.FileType("rt"),
            default=sys.stdin,
            nargs="?",
            help="Path to ISA-tab investigation file.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, _args):
        """Called for checking arguments, override to change behaviour."""
        return 0

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk isa-tab resolve-hpo")
        logger.info("  args: %s", self.args)

        ok = True

        with tempfile.TemporaryDirectory() as tmpdir:
            path_obo = os.path.join(tmpdir, "hp.obo")
            try:
                download_with_progress(self.args.hpo_obo_url, path_obo)
            except ResourceDownloadError as e:
                logger.error("Problem downloading file: %s", e)
                return 1

            logger.info("Loading HPO from %s", path_obo)
            graph, _alt_ids, _obsolete = hpo_similarity.ontology.open_ontology(path_obo)

            logger.info("Resolving HPO terms...")
            for line in self.args.term_file:
                arr = [fixup(x.strip()) for x in re.split("[;,]", line.strip())]
                arr = [x for x in arr if x]
                names = [
                    escape(
                        graph._node.get(term, {}).get(  # pylint: disable=protected-access
                            "name", missing(term)
                        )
                    )
                    for term in arr
                    if term
                ]
                urls = [escape("http://purl.obolibrary.org/obo/%s" % term) for term in arr if term]
                ontology_refs = ["HP"] * len(arr)
                record = (";".join(arr), ";".join(names), ";".join(ontology_refs), ";".join(urls))
                if len(urls) != len(names):
                    ok = False
                    print('WARNING: len(urls) != len(names")', file=sys.stderr)
                print("\t".join(record))

        return int(not ok)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-raw-data``."""
    return ResolveHpoCommand.setup_argparse(parser)
