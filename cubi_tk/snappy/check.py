"""``cubi-tk snappy check``: check within sample sheet and between sample sheet and files."""

import argparse
import glob
import os
import pathlib
import typing
import warnings

from biomedsheets import shortcuts
from logzero import logger
import vcfpy

from .itransfer_common import load_sheets_tsv
from .. import parse_ped


class GermlineSheetChecker:
    """Helper class that implements the consistency checks within germline sheets."""

    def __init__(self, sheets: typing.Iterable[shortcuts.GermlineCaseSheet]):
        #: Shortcut sheet.
        self.sheets = list(sheets)

    def run_checks(self):
        """Execute checks, return True if all good else False."""
        logger.info("Running germline sheet checks...")
        results = []
        for sheet in self.sheets:
            results += [
                self._check_parent_sex(sheet),
                self._check_dangling_parents(sheet),
                self._check_family_id(sheet),
            ]
        return all(results)

    def _check_parent_sex(self, sheet: shortcuts.GermlineCaseSheet):
        """Check whether parent sex is consistent."""
        logger.info("Checking for parent sex consistency...")
        ok = True

        name_to_sex = {}
        fathers = set()
        father_of: typing.Dict[str, typing.Set[str]] = {}
        mothers = set()
        mother_of: typing.Dict[str, typing.Set[str]] = {}

        for donor in sheet.donors:
            if donor.father:
                fathers.add(donor.father.name)
                father_of.setdefault(donor.father.name, set()).add(donor.name)
            if donor.mother:
                mothers.add(donor.mother.name)
                mother_of.setdefault(donor.mother.name, set()).add(donor.name)
            name_to_sex[donor.name] = donor.extra_infos.get("sex", "unknown")

        for name in fathers:
            sex = name_to_sex.get(name, "unknown")
            if sex != "male":
                logger.warning(
                    "Donor %s is father of %s but sex is % and not male",
                    name,
                    ", ".join(sorted(father_of[name])),
                    sex,
                )
                ok = False
        for name in mothers:
            sex = name_to_sex.get(name, "unknown")
            if sex != "female":
                logger.warning(
                    "Donor %s is mother of %s but sex is % and not female",
                    name,
                    ", ".join(sorted(mother_of[name])),
                    sex,
                )
                ok = False

        return ok

    def _check_dangling_parents(self, sheet: shortcuts.GermlineCaseSheet):
        """Check whether there are any dangling parents."""
        logger.info("Checking for dangling parents...")
        ok = True

        donor_names = {donor.name for donor in sheet.donors}
        for donor in sheet.donors:
            if donor.father and donor.father.name not in donor_names:
                logger.warning("Father of %s is not known: %s", donor.father.name, donor.name)
                ok = False
            if donor.mother and donor.mother.name not in donor_names:
                logger.warning("Mother of %s is not known: %s", donor.father.name, donor.name)
                ok = False

        return ok

    def _check_family_id(self, sheet: shortcuts.GermlineCaseSheet):
        """Check whether parents links point over family boundaries."""
        ok = True

        seen_family_ids: typing.Set[str] = set()

        for pedigree in sheet.cohort.pedigrees:
            if pedigree.index.extra_infos.get("familyId"):
                if pedigree.index.extra_infos.get("familyId") in seen_family_ids:
                    # TODO: in the future this will be OK once we do not need linking fake entries for snappy any more.
                    logger.warning("Family seen for two unconnected pedigrees")
                    ok = False
            family_ids = {donor.extra_infos.get("familyId") for donor in pedigree.donors}
            seen_family_ids |= family_ids
            if len(family_ids) != 1:
                logger.warning(
                    "Seen multiple family IDs within one pedigree: %s",
                    ", ".join(sorted(family_ids)),
                )
                ok = False

        no_family_donors = {
            donor for donor in sheet.donors if not donor.extra_infos.get("familyId")
        }
        if no_family_donors:
            logger.warning(
                "Found donors without family ID: %s",
                ", ".join(sorted([donor.name for donor in no_family_donors])),
            )
            ok = False

        return ok


class FileCheckBase:
    """Base class for file checks."""

    # Patterns to glob by.
    file_globs: typing.Optional[typing.Tuple[str, ...]] = None

    def get_file_globs(self):
        return self.file_globs

    def __init__(self, sheets: typing.Iterable[shortcuts.GermlineCaseSheet], base_dir: str):
        #: Germline shortcut sheet.
        self.sheets = list(sheets)
        #: Base directory to search steps in.
        self.base_dir = base_dir
        #: Merge dicts.
        self.donor_ngs_library_to_pedigree: typing.Dict[str, shortcuts.Pedigree] = {}
        for sheet in self.sheets:
            self.donor_ngs_library_to_pedigree.update(sheet.donor_ngs_library_to_pedigree)

    def run_checks(self):
        """Execute checks, return True if all good else False."""
        logger.info("Running germline sheet checks...")
        return self._check_files()

    def _check_files(self):
        """Find all files using the glob, infer index from file name and check them."""
        results = []
        for file_glob in self.get_file_globs():
            results.append(self._check_files_inner(file_glob))
        return all(results)

    def _check_files_inner(self, file_glob):
        results = []
        for file_path in glob.glob(os.path.join(self.base_dir, file_glob)):
            logger.debug("Checking %s", file_path)
            results.append(self.check_file(file_path))
        return all(results)

    def check_file(self, path):
        """Check one file, returning True on success, False on errors."""
        raise NotImplementedError("Override me!")


class PedFileCheck(FileCheckBase):
    """Implement consistency checks between germline sheet and .ped files."""

    file_globs = ("*/work/write_pedigree.*/**/*.ped",)

    def check_file(self, path):
        ped_path = path
        results = []

        with open(ped_path, "rt") as pedf:
            donors = list(parse_ped.parse_ped(pedf))
            if not donors:
                logger.error("Empty pedigree file %s", ped_path)
                results.append(False)
                return False
            else:
                ped_names = {donor.name for donor in donors}
                pedigree = self.donor_ngs_library_to_pedigree.get(donors[0].name)
                if not pedigree:
                    logger.error(
                        (
                            "Found pedigree in PED file but not in sample sheet. \n\n"
                            "index:    %s\n"
                            "PED file: %s\n"
                        ),
                        donors[0].name,
                        os.path.relpath(ped_path, self.base_dir),
                    )
                    results.append(False)
                    return False
                pedigree_names = {
                    donor.dna_ngs_library.name for donor in pedigree.donors if donor.dna_ngs_library
                }
                if ped_names != pedigree_names:
                    logger.error(
                        (
                            "Members in PED file differ from members in sample sheet.\n\n"
                            "shared:     %s\n"
                            "sheet only: %s\n"
                            "PED only:   %s\n"
                            "PED file:   %s\n"
                        ),
                        ",".join(sorted(pedigree_names & ped_names)) or "(none)",
                        ",".join(sorted(pedigree_names - ped_names)) or "(none)",
                        ",".join(sorted(ped_names - pedigree_names)) or "(none)",
                        os.path.relpath(ped_path, self.base_dir),
                    )
                    results.append(False)
                    return False
                for ped_donor in donors:
                    sheet_donor = self._get_sheet_donor(ped_donor, ped_path)
                    if sheet_donor:
                        results.append(
                            self._check_parent(ped_donor, sheet_donor, "father", ped_path)
                        )
                        results.append(
                            self._check_parent(ped_donor, sheet_donor, "mother", ped_path)
                        )
                        results.append(
                            self._check_sex_disease(ped_donor, sheet_donor, "sex", ped_path)
                        )
                        results.append(
                            self._check_sex_disease(ped_donor, sheet_donor, "disease", ped_path)
                        )

        return all(results)

    def _get_sheet_donor(self, ped_donor, ped_path):
        sheet_pedigree = self.donor_ngs_library_to_pedigree.get(ped_donor.name)
        if not sheet_pedigree:
            logger.error(
                "Found no pedigree in sample sheet for PED donor.\n\nPED donor: %s\nPED file:  %s\n",
                ped_donor.name,
                os.path.relpath(ped_path, self.base_dir),
            )
            return None
        for tmp in sheet_pedigree.donors:
            if tmp.dna_ngs_library and tmp.dna_ngs_library.name == ped_donor.name:
                return tmp
        logger.error(
            "Member in PED not found in sample sheet.\n\nPED donor: %s\nPED file:  %s\n",
            ped_donor.name,
            os.path.relpath(ped_path, self.base_dir),
        )
        return None

    def _check_parent(self, ped_donor, sheet_donor, key, ped_path):
        if (getattr(sheet_donor, key) is None) != (getattr(ped_donor, "%s_name" % key) == "0"):
            logger.error(
                "Inconsistent %s between PED and sample sheet.\n\ndonor:    %s\nPED file: %s\n",
                key,
                ped_donor.name,
                os.path.relpath(ped_path, self.base_dir),
            )
            return False
        elif getattr(sheet_donor, key):
            if not getattr(sheet_donor, key).dna_ngs_library:
                logger.error(
                    "Sheet donor's %s does not have library.\n\ndonor:    %s\nPED file: %s\n",
                    key,
                    sheet_donor.name,
                    os.path.relpath(ped_path, self.base_dir),
                )
                return False
            elif getattr(sheet_donor, key).dna_ngs_library.name != getattr(
                ped_donor, "%s_name" % key
            ):
                logger.error(
                    (
                        "Inconsistent %s name between PED and sample sheet.\n\n"
                        "donor:    %s\n"
                        "in sheet: %s\n"
                        "in PED:   %s\n"
                        "PED file: %s\n"
                    ),
                    key,
                    ped_donor.name,
                    getattr(sheet_donor, key).dna_ngs_library.name,
                    getattr(ped_donor, "%s_name" % key),
                    os.path.relpath(ped_path, self.base_dir),
                )
                return False
            return True
        else:
            return True

    def _check_sex_disease(self, ped_donor, sheet_donor, key, ped_path):
        key2 = {"sex": "sex", "disease": "isAffected"}[key]
        if getattr(ped_donor, key) != sheet_donor.extra_infos.get(key2, "unknown"):
            logger.error(
                (
                    "Inconsistent %s between PED and sample sheet.\n\n"
                    "donor:    %s\n"
                    "in sheet: %s\n"
                    "in PED:   %s\n"
                    "PED file: %s\n"
                ),
                key,
                ped_donor.name,
                sheet_donor.extra_infos.get(key, "unknown"),
                getattr(ped_donor, key),
                os.path.relpath(ped_path, self.base_dir),
            )
            return False
        else:
            return True


class VcfFileCheck(FileCheckBase):
    """Implement consistency check between germline sheet and .vcf.gz files."""

    def get_file_globs(self):
        result = [
            "*/output/{mapper}.{caller}.*/**/*.vcf.gz".format(mapper=mapper, caller=caller)
            for mapper in ("bwa",)
            for caller in ("gatk_hc", "gatk_ug", "xhmm")
        ]
        return result

    def check_file(self, path):
        vcf_path = path
        real_path = os.path.realpath(vcf_path)
        if not os.path.exists(real_path):
            logger.error(
                "Symlink problem, points to non-existing path\n\nlink: %s\ndest: %s\n",
                os.path.relpath(vcf_path, self.base_dir),
                os.path.relpath(real_path, self.base_dir),
            )
            return False
        with warnings.catch_warnings():  # suppress warnings
            warnings.simplefilter("ignore")
            with vcfpy.Reader.from_path(vcf_path) as reader:
                vcf_names = reader.header.samples.names
                if not vcf_names:
                    logger.error(
                        "Found no samples in VCF path\n\nVCF path: %s",
                        os.path.relpath(vcf_path, self.base_dir),
                    )
                    return False
                pedigree = self.donor_ngs_library_to_pedigree.get(vcf_names[0])
                if not pedigree:
                    logger.error(
                        "Index from VCF not found in sample sheet.\n\nindex:    %s\nVCF path: %s\n",
                        vcf_names[0],
                        os.path.relpath(vcf_path, self.base_dir),
                    )
                    return False
                vcf_names = set(vcf_names)
                pedigree_names = {
                    donor.dna_ngs_library.name for donor in pedigree.donors if donor.dna_ngs_library
                }
                if vcf_names != pedigree_names:
                    logger.error(
                        (
                            "Inconsistent members between VCF and sample sheets.\n\n"
                            "shared:     %s\n"
                            "VCF only:   %s\n"
                            "sheet only: %s\n"
                            "VCF path:   %s\n"
                        ),
                        ", ".join(sorted(vcf_names & pedigree_names)) or "(none)",
                        ", ".join(sorted(vcf_names - pedigree_names)) or "(none)",
                        ", ".join(sorted(pedigree_names - vcf_names)) or "(none)",
                        os.path.relpath(vcf_path, self.base_dir),
                    )
                    return False
        return True


class SnappyCheckCommand:
    """Implementation of the ``check`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        #: Raw sample sheet.
        self.sheets = load_sheets_tsv(self.args)
        #: Shortcut sample sheet.
        self.shortcut_sheets = [shortcuts.GermlineCaseSheet(sheet) for sheet in self.sheets]

    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup common arguments for itransfer commands."""
        parser.add_argument(
            "--hidden-cmd", dest="snappy_cmd", default=cls.run, help=argparse.SUPPRESS
        )

        parser.add_argument(
            "--tsv-shortcut",
            default="germline",
            choices=("germline", "cancer"),
            help="The shortcut TSV schema to use.",
        )
        parser.add_argument(
            "--base-path",
            default=None,
            required=False,
            help=(
                "Base path of project (contains 'ngs_mapping/' etc.), spiders up from biomedsheet_tsv and falls "
                "back to current working directory by default."
            ),
        )

        parser.add_argument(
            "biomedsheet_tsv",
            nargs="+",
            type=argparse.FileType("rt"),
            help="Path to biomedsheets TSV file to load.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments, override to change behaviour."""
        res = 0

        for tsv_file in args.biomedsheet_tsv:
            if args.base_path:
                break
            base_path = pathlib.Path(tsv_file.name).parent
            while base_path != base_path.root:
                if (base_path / "ngs_mapping").exists():
                    args.base_path = str(base_path)
                    break
                base_path = base_path.parent
        if not args.base_path:
            args.base_path = os.getcwd()

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy check")
        logger.info("  args: %s", self.args)

        results = [
            GermlineSheetChecker(self.shortcut_sheets).run_checks(),
            PedFileCheck(self.shortcut_sheets, self.args.base_path).run_checks(),
            VcfFileCheck(self.shortcut_sheets, self.args.base_path).run_checks(),
        ]

        logger.info("All done")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy itransfer-raw-data``."""
    return SnappyCheckCommand.setup_argparse(parser)
