"""``cubi-tk sodar ingest-fastq``: add FASTQ files to SODAR"""

import argparse
from ctypes import c_ulonglong
import datetime
import glob
from multiprocessing import Value
from multiprocessing.pool import ThreadPool
import os
import pathlib
import re
from subprocess import SubprocessError, check_call, check_output
import sys
import typing

from loguru import logger
import tqdm

from cubi_tk.parsers import print_args
from cubi_tk.sodar_api import SodarApi

from ..common import sizeof_fmt
from ..exceptions import MissingFileException, ParameterException, UserCanceledException
from ..irods_common import TransferJob, iRODSTransfer
from ..snappy.itransfer_common import SnappyItransferCommandBase

# for testing
logger.propagate = True


SRC_REGEX_PRESETS = {
    "default": (
        r"(.*/)?(?P<sample>.+?)"
        r"(?:_S[0-9]+)?"
        r"(?:_(?P<lane>L[0-9]+?))?"
        r"(?:_(?P<mate>R[0-9]+?))?"
        r"(?:_(?P<batch>[0-9]+?))?"
        r"\.f(?:ast)?q\.gz"
    ),
    "digestiflow": (
        r"(.*/)?(?P<flowcell>[A-Z0-9]{9,10}?)/"
        r"(?P<lane>L[0-9]{3}?)/"
        r"(?:(?P<project>[A-Z][0-9]+_?))?"
        r"(?P<sample>.+?)_"
        r"S[0-9]+_L[0-9]{3}_R[0-9]_[0-9]{3}"
        r"\.fastq\.gz"
    ),
    "ONT": (
        r"(.*/)?"
        r"[0-9]{8}_"  # Date
        # Sample could be <ProjectID>_<LibID>_<SampleID>, but this is not given and may change between projects
        r"(?P<sample>[a-zA-Z0-9_-]+?)/"
        # RunID is <date>_<time>_<position>_<flowcellID>_<hash>
        # Flowcells can be re-used, so taking the whole thing for uniqueness is best
        r"(?P<RunID>[0-9]{8}_[0-9]+_[A-Z0-9]+_[A-Z0-9]+_[0-9a-z]+?)/"
        r"(?:(?P<subfolder>[a-z0-9_]+/))?"
        r".+\.(bam|pod5|txt|json)"
    ),
    "onk_analysis":(
        r"(.*/)?(?:(UMI_collapsing_)?(?P<sample>[a-zA-Z0-9_-]+))"
        r"_(?P<conservation>(FF|FFPE))"
        r"(?:(.*?)?(?P<tissue>(tumor|normal)))?"
        r"\.(bam|.*bed$|.*bed.gz|txt|json|.*vcf)"
    )
}

DEST_PATTERN_PRESETS = {
    "default": r"{collection_name}/raw_data/{date}/{filename}",
    "digestiflow": r"{collection_name}/raw_data/{flowcell}/{filename}",
    "ONT": r"{collection_name}/raw_data/{RunID}/{subfolder}/{filename}",
    "onk_analysis": r"{collection_name}/analysis/{date}/{filename}"
}

#: Default number of parallel transfers.
DEFAULT_NUM_TRANSFERS = 8


def compute_md5sum(job: TransferJob, counter: Value, t: tqdm.tqdm) -> None:
    """Compute MD5 sum with ``md5sum`` command."""
    dirname = os.path.dirname(job.path_local)
    filename = os.path.basename(job.path_local)[: -len(".md5")]
    path_md5 = job.path_local

    md5sum_argv = ["md5sum", filename]
    logger.debug("Computing MD5sum {} > {}", " ".join(md5sum_argv), filename + ".md5")
    try:
        with open(path_md5, "wt") as md5f:
            check_call(md5sum_argv, cwd=dirname, stdout=md5f)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing md5sum: {}", e)
        logger.info("Removing file after error: {}", path_md5)
        try:
            os.remove(path_md5)
        except OSError as e_rm:  # pragma: nocover
            logger.error("Could not remove file: {}", e_rm)
        raise e

    with counter.get_lock():
        counter.value = os.path.getsize(job.path_local[: -len(".md5")])
        try:
            t.update(counter.value)
        except TypeError:
            pass  # swallow, pyfakefs and multiprocessing don't lik each other


class SodarIngestFastq(SnappyItransferCommandBase):
    """Implementation of sodar ingest-fastq command."""

    fix_md5_files = True
    command_name = "ingest-fastq"

    def __init__(self, args):
        super().__init__(args)
        if self.args.remote_dir_pattern:
            self.remote_dir_pattern = self.args.remote_dir_pattern
        else:
            self.remote_dir_pattern = DEST_PATTERN_PRESETS[self.args.preset]
        self.dest_pattern_fields = set(re.findall(r"(?<={).+?(?=})", self.remote_dir_pattern))

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument(
            "--hidden-cmd", dest="sodar_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--num-parallel-transfers",
            type=int,
            default=DEFAULT_NUM_TRANSFERS,
            help="Number of parallel transfers, defaults to %s" % DEFAULT_NUM_TRANSFERS,
        )
        parser.add_argument(
            "-s",
            "--sync",
            default=False,
            action="store_true",
            help="Skip upload of files already present in remote collection.",
        )

        parser.add_argument(
            "--yes",
            default=False,
            action="store_true",
            help="Assume the answer to all prompts is 'yes'",
        )
        parser.add_argument(
            "--validate-and-move",
            default=False,
            action="store_true",
            help="After files are transferred to SODAR, it will proceed with validation and move.",
        )
        parser.add_argument(
            "--preset",
            default="default",
            choices=DEST_PATTERN_PRESETS.keys(),
            help=f"Use predefined values for regular expression to find local files (--src-regex) and pattern to for "
            f"constructing remote file paths.\nDefault src-regex: {SRC_REGEX_PRESETS['default']}.\n"
            f"Default --remote-dir-pattern: {DEST_PATTERN_PRESETS['default']}.",
        )
        parser.add_argument(
            "--src-regex",
            default=None,
            help="Manually defined regular expression to use for matching input fastq files. Takes precedence over "
            "--preset.  This regex controls which files are ingested, so it can be used for any file type. "
            "Any named capture group in the regex can be used with --remote-dir-pattern. The 'sample' group is "
            "used to set irods collection names (as-is or via --match-column).",
        )
        parser.add_argument(
            "--remote-dir-pattern",
            default=None,
            help="Manually defined pattern to use for constructing remote file paths. Takes precedence over "
            "--preset. 'collection_name' is the target iRODS collection and will be filled with the (-m regex "
            "modified) 'sample', or if --match-column is used with the corresponding value from the  assay table. "
            "Any capture group of the src-regex ('sample', 'lane', ...) can be used along with 'date' and 'filename'.",
        )
        parser.add_argument(
            "--match-column",
            default=None,
            help="Alternative assay column against which the {sample} from the src-regex should be matched, "
            "in order to determine collections based on the assay table (e.g. last material or collection-column). "
            "If not set it is assumed that {sample} matches the iRODS collections directly. If it matches multiple "
            "columns the last one can be used.",
        )
        parser.add_argument(
            "-m",
            "--sample-collection-mapping",
            nargs=2,
            action="append",
            metavar=("MATCH", "REPL"),
            default=[],
            type=str,
            help="Substitutions applied to the extracted sample name, "
            "which is used to determine iRODS collections."
            "Can be used to change extracted string to correct collections names "
            "or to match the values of '--match-column'."
            "Use pythons regex syntax of 're.sub' package. "
            "This argument can be used multiple times "
            "(i.e. '-m <regex1> <repl1> -m <regex2> <repl2>' ...).",
        )
        parser.add_argument(
            "--remote-dir-date",
            default=datetime.date.today().strftime("%Y-%m-%d"),
            help="Date to use in remote directory, defaults to YYYY-MM-DD of today.",
        )
        parser.add_argument(
            "--collection-column",
            default=None,
            help="Assay column from that matches iRODS collection names. "
            "If not set, the last material column will be used. If it matches multiple "
            "columns the last one can be used.",
        )
        parser.add_argument(
            "--tmp",
            default="temp/",
            help="Folder to save files from WebDAV temporarily, if set as source.",
        )

        parser.add_argument("sources", help="paths to fastq folders", nargs="+")

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        if args.src_regex and args.remote_dir_pattern and args.preset != "default":
            logger.error(
                "Using both --src-regex and --remote-dir-pattern at the same time overwrites all values defined "
                "by --preset. Please drop the use of --preset or at least one of the other manual definitions."
            )
            res = 1

        return res


    def build_base_dir_glob_pattern(self, library_name: str) -> tuple[str, str]:
        raise NotImplementedError(
            "build_base_dir_glob_pattern() not implemented in SodarIngestFastq!"
        )

    def get_match_to_collection_mapping(
        self, sodar_api: SodarApi, in_column: str, out_column: typing.Optional[str] = None
    ) -> dict[str, str]:
        """Return a dict that matches all values from a specific `ìn_column` of the assay table
        to a corresponding `out_column` (default if not defined: last Material column)."""
        isa_dict = sodar_api.get_samplesheet_export()
        in_column_dict = None
        out_column_dict= None

        for sheet_type in ["assays", "studies"]:
            sheet_file_name = list(isa_dict[sheet_type].keys())[0]
            sheet_tsv = isa_dict[sheet_type][sheet_file_name]["tsv"]
            sheet_header, *sheet_lines = sheet_tsv.rstrip("\n").split("\n")
            sheet_header = sheet_header.split("\t")
            sheet_lines =  [x.split("\t") for x in sheet_lines]

            sample_name_idx, in_column_index, out_column_index, conservation_method_idx = self._get_indices(sheet_header, in_column, out_column)
            if in_column_dict is None:
                in_column_dict = self._check_col_index_and_get_val(in_column_index, sheet_type, sheet_header, sheet_lines, sample_name_idx, conservation_method_idx)
            if out_column_dict is None:
                if out_column is None:
                    #take last extractname without asking user
                    out_column_index = [max(out_column_index)]
                out_column_dict = self._check_col_index_and_get_val(out_column_index, sheet_type, sheet_header, sheet_lines, sample_name_idx, conservation_method_idx)

            #dont go into studies
            if in_column_dict is not None and out_column_dict is not None:
                continue
        if in_column_dict is None:
            msg = "Could not identify any column in the assay or study sheet matching provided data. Please review input: --match-column={}".format(
                        in_column
                    )
            logger.error(msg)
            raise ParameterException
        #assuming that outcolumn is in study
        match_dict = {}
        for sample_name in out_column_dict.keys():
            if sample_name in in_column_dict.keys():
                key = in_column_dict[sample_name]
                if isinstance(key, list):
                    #add sample name for tumor/normal check and key[1] conservationmethod
                    #dont overwrite matchdict key if multiple present
                    val = match_dict.get(key[0], [])
                    val.append((out_column_dict[sample_name], sample_name, key[1]))
                    match_dict[key[0]] = val
                else:
                    match_dict[key] = out_column_dict[sample_name]
        return match_dict

    def _get_indices(self,sheet_header, in_column, out_column):
        # Never match these (hidden) assay columns
        ignore_cols = (
            "Performer",
            "Date",
            "Protocol REF",
            "Unit",
            "Term Source REF",
            "Term Accession Number",
        )
        materials = (
            "Extract Name",
            "Labeled Extract Name",
            "Library Name",
            "Sample Name",
            "Source Name",
        )
        sample_name_idx = None
        conservation_method_idx = None
        in_column_index = []
        out_column_index = []

        for i, head in enumerate(sheet_header):
            if head not in ignore_cols:
                search_vals = re.sub("(Parameter Value|Comment|Characteristics)", "", head).lower()
                if in_column.lower() in search_vals:
                    in_column_index.append(i)
                if out_column is not None and out_column.lower() in search_vals:
                    out_column_index.append(i)
            if "Sample Name" in head:
                sample_name_idx = i
            if "Conservation method" in head:
                conservation_method_idx = i
            # Get index of last material column that is not 'Raw Data File'
            if out_column is None and head in materials:
                out_column_index.append(i)

        return sample_name_idx, in_column_index, out_column_index, conservation_method_idx

    def _check_col_index_and_get_val(self, column_index, sheet_type, sheet_header, sheet_lines, sample_name_idx, conservation_method_idx):
        if not column_index:
            msg = "Could not identify any column in the {} sheet matching provided data.".format(
                sheet_type[:-1]
            )
            logger.info(msg)
            return None
        elif len(column_index) > 1:
            column_index = max(column_index)
            if self.args.yes:
                logger.info(
                    "Multiple columns in the {} sheet match the provided column name ({}), using the last one.".format(
                        sheet_type[:-1], sheet_header[column_index]
                    )
                )
            elif (
                input(
                    "Multiple columns in the {} sheet match the provided column name ({}), use the last one? [yN] ".format(
                        sheet_type[:-1], sheet_header[column_index]
                    )
                )
                .lower()
                .startswith("y")
            ):
                pass
            else:
                msg = "Not possible to continue the process without a defined match-column. Breaking..."
                logger.info(msg)
                raise UserCanceledException(msg)
        else:
            column_index = column_index[0]
        column_dict: dict[str,str] = {}
        for line in sheet_lines:
            if conservation_method_idx is not None and self.args.preset == "onk_analysis":
                #add conservation method
                column_dict[line[sample_name_idx]] = [line[column_index], line[conservation_method_idx]]
            else:
                column_dict[line[sample_name_idx]] = line[column_index]
        return column_dict

    def download_webdav(self, sources):
        download_jobs = []
        folders = []
        for src in sources:
            if re.match("davs://", src):
                download_jobs.append(TransferJob(path_remote=src, path_local=self.args.tmp))
                tmp_folder = f"tmp_folder_{len(download_jobs)}"
                pathlib.Path(tmp_folder).mkdir(parents=True, exist_ok=True)
            else:
                folders.append(src)

        if download_jobs:
            logger.info("Planning to download folders...")
            for job in download_jobs:
                logger.info("{} => {}", job.path_remote, job.path_local)
            if not self.args.yes and not input("Is this OK? [yN] ").lower().startswith("y"):
                logger.error("OK, breaking at your request")
                return []

            counter = Value(c_ulonglong, 0)
            total_bytes = sum([job.bytes for job in download_jobs])
            with tqdm.tqdm(total=total_bytes) as t:
                if self.args.num_parallel_transfers == 0:  # pragma: nocover
                    for job in download_jobs:
                        download_folder(job, counter, t)
                else:
                    pool = ThreadPool(processes=self.args.num_parallel_transfers)
                    for job in download_jobs:
                        pool.apply_async(download_folder, args=(job, counter, t))
                    pool.close()
                    pool.join()

        return folders

    def find_collection_name(self, sample_name, column_match, m):
        if column_match is None:
            return sample_name
        val = column_match[sample_name]
        if not isinstance(val, list):
            return val
        #needs further matching with tumor and conservation method
        conservation = m.groupdict(default=None)["conservation"]
        tissue = m.groupdict(default=None)["tissue"]
        for col_col_name, col_sample_name, col_conservation in val:
            col_tissue = col_sample_name.split("-")[-1][0]
            tissue_match = tissue =="tumor" and col_tissue =="T" or tissue == "normal" and col_tissue  == "N"
            conservation_match = conservation == col_conservation or conservation == "FF" and col_conservation == "fresh frozen"
            if conservation is not None and tissue is not None and tissue_match and conservation_match:
                return col_col_name
            elif conservation is not None and tissue is None and conservation_match:
                return col_col_name
            elif tissue is not None and conservation is None and tissue_match:
                return col_col_name
        logger.info("Couldnt match to conservation and/or tissue, returning first")
        return val[0][0]


    def build_jobs(self, library_names=None) -> tuple[str, tuple[TransferJob, ...]]:  # noqa: C901
        """Build file transfer jobs."""
        if library_names:
            logger.warning(
                "will ignore parameter 'library_names' = {} in ingest_fastq.build_jobs()",
                str(library_names),
            )
        sodar_api = SodarApi(self.args, with_dest=True, dest_string="destination")
        try:
            lz_uuid, lz_irods_path = self.get_sodar_info(sodar_api)
        except ParameterException as e:
            logger.error(f"Couldn't find LZ UUID and LZ iRods Path: {e}")
            sys.exit(1)

        sodar_api.get_landingzone_retrieve(lz_uuid=lz_uuid) #sets project uuid in sodar_api
        if self.args.match_column is not None:
            column_match = self.get_match_to_collection_mapping(
                sodar_api, self.args.match_column, self.args.collection_column
            )
        else:
            column_match = None

        folders = self.download_webdav(self.args.sources)
        transfer_jobs = []

        if self.args.src_regex:
            use_regex = re.compile(self.args.src_regex)
        else:
            use_regex = re.compile(SRC_REGEX_PRESETS[self.args.preset])
        # logger.debug(f"Using regex: {use_regex}")

        for folder in folders:
            for path in glob.iglob(f"{folder}/**/*", recursive=True):
                real_path = os.path.realpath(path)
                if not os.path.isfile(real_path):
                    continue  # skip if did not resolve to file
                if real_path.endswith((".md5", ".md5sum")):
                    continue  # skip, will be added automatically

                if not os.path.exists(real_path):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % real_path)
                if (
                    not os.path.exists(real_path + ".md5") and not self.fix_md5_files
                ):  # pragma: nocover
                    raise MissingFileException("Missing file %s" % (real_path + ".md5"))

                # logger.debug(f"Checking file: {path}")
                m = re.match(use_regex, path)
                if m:
                    logger.debug("Matched {} with regex {}: {}", path, use_regex, m.groupdict())
                    match_wildcards = dict(
                        item
                        for item in m.groupdict(default="").items()
                        if item[0] in self.dest_pattern_fields
                    )

                    # `-m` regex now only applied to extracted sample name
                    sample_name = m.groupdict(default="")["sample"]
                    for m_pat, r_pat in self.args.sample_collection_mapping:
                        sample_name = re.sub(m_pat, r_pat, sample_name)
                    try:
                        collection_name = self.find_collection_name(sample_name, column_match, m)
                        remote_file = pathlib.Path(lz_irods_path) / self.remote_dir_pattern.format(
                            # Removed the `+ self.args.add_suffix` here, since adding anything after the file extension is a bad idea
                            filename=pathlib.Path(path).name,
                            date=self.args.remote_dir_date,
                            collection_name=collection_name,
                            **match_wildcards,
                        )
                    except KeyError:
                        msg = (
                            f"Could not match extracted sample value '{sample_name}' to any value in the "
                            f"--match-column {self.args.match_column}. Please review the assay table, src-regex and sample-collection-mapping args."
                        )
                        logger.error(msg)
                        raise ParameterException(msg) from KeyError

                    # This was the original code, but there is no need to change the remote file names once they are
                    # mapped to the correct collections:
                    # remote_file = str(remote_file)
                    # for m_pat, r_pat in self.args.remote_dir_mapping:
                    #     remote_file = re.sub(m_pat, r_pat, remote_file)

                    for ext in ("", ".md5"):
                        transfer_jobs.append(
                            TransferJob(
                                path_local=real_path + ext,
                                path_remote=str(remote_file) + ext,
                            )
                        )

        return lz_irods_path, tuple(sorted(transfer_jobs, key=lambda x: x.path_local))

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk sodar {}", self.command_name)
        print_args(self.args)

        lz_uuid, transfer_jobs = self.build_jobs()
        transfer_jobs = sorted(transfer_jobs, key=lambda x: x.path_local)
        # Exit early if no files were found/matched
        if not transfer_jobs:
            if self.args.src_regex:
                used_regex = self.args.src_regex
            else:
                used_regex = SRC_REGEX_PRESETS[self.args.preset]

            logger.warning("No matching files were found!\nUsed regex: {}", used_regex)
            return None

        if self.fix_md5_files:
            transfer_jobs = self._execute_md5_files_fix(transfer_jobs)

        # Final go from user & transfer
        itransfer = iRODSTransfer(transfer_jobs, ask=not self.args.yes)
        logger.info("Planning to transfer the following files:")
        for job in transfer_jobs:
            logger.info(job.path_local)
        logger.info(f"With a total size of {sizeof_fmt(itransfer.size)}")

        if not self.args.yes:
            if not input("Is this OK? [y/N] ").lower().startswith("y"):  # pragma: no cover
                logger.info("Aborting at your request.")
                sys.exit(0)

        itransfer.put(recursive=True, sync=self.args.sync, yes=self.args.yes)
        logger.info("File transfer complete.")

        # Validate and move transferred files
        # Behaviour: If flag is True and lz uuid is not None*,
        # it will ask SODAR to validate and move transferred files.
        # (*) It can be None if user provided path
        if lz_uuid and self.args.validate_and_move:
            self.move_landing_zone(lz_uuid=lz_uuid)
        else:
            logger.info("Transferred files will \033[1mnot\033[0m be automatically moved in SODAR.")

        logger.info("All done")
        return None

    def _execute_md5_files_fix(
        self, transfer_jobs: typing.Tuple[TransferJob, ...]
    ) -> typing.Tuple[TransferJob, ...]:
        """Create missing MD5 files."""
        ok_jobs = []
        todo_jobs = []
        for job in transfer_jobs:
            if not os.path.exists(job.path_local):
                todo_jobs.append(job)
            else:
                ok_jobs.append(job)

        total_bytes = sum([os.path.getsize(j.path_local[: -len(".md5")]) for j in todo_jobs])
        logger.info(
            "Computing MD5 sums for {} files of {} with up to {} processes",
            len(todo_jobs),
            sizeof_fmt(total_bytes),
            self.args.num_parallel_transfers,
        )
        logger.info("Missing MD5 files:\n{}", "\n".join( j.path_local for j in todo_jobs))
        counter = Value(c_ulonglong, 0)
        with tqdm.tqdm(total=total_bytes, unit="B", unit_scale=True) as t:
            if self.args.num_parallel_transfers == 0:  # pragma: nocover
                for job in todo_jobs:
                    compute_md5sum(job, counter, t)
            else:
                pool = ThreadPool(processes=self.args.num_parallel_transfers)
                for job in todo_jobs:
                    pool.apply_async(compute_md5sum, args=(job, counter, t))
                pool.close()
                pool.join()

        # Finally, determine file sizes after done.
        done_jobs = [
            TransferJob(
                path_local=j.path_local,
                path_remote=j.path_remote,
            )
            for j in todo_jobs
        ]
        return tuple(sorted(done_jobs + ok_jobs, key=lambda x: x.path_local))


def download_folder(job: TransferJob, counter: Value, t: tqdm.tqdm):
    """Perform one piece of work and update the global counter."""

    irsync_argv = ["irsync", "-r", "-a", "-K", "i:%s" % job.path_remote, job.path_local]
    logger.debug("Transferring file: {}", " ".join(irsync_argv))
    try:
        check_output(irsync_argv)
    except SubprocessError as e:  # pragma: nocover
        logger.error("Problem executing irsync: {}", e)
        raise

    with counter.get_lock():
        counter.value = job.bytes
        t.update(counter.value)


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk org-raw check``."""
    return SodarIngestFastq.setup_argparse(parser)
