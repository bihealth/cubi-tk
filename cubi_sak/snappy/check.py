"""``cubi-sak snappy check``: check within sample sheet and between sample sheet and files."""

import argparse
import glob
import os
import typing
import warnings

from biomedsheets import shortcuts
from logzero import logger
import vcfpy

from .itransfer_common import load_sheet_tsv
from .. import parse_ped


class GermlineSheetChecker:
    """Helper class that implements the consistency checks within germline sheets."""

    def __init__(self, sheet: shortcuts.GermlineCaseSheet):
        #: Shortcut sheet.
        self.sheet = sheet

    def run_checks(self):
        """Execute checks, return True if all good else False."""
        logger.info("Running germline sheet checks...")
        results = [
            self._check_parent_sex(),
            self._check_dangling_parents(),
            self._check_family_id(),
        ]
        return all(results)

    def _check_parent_sex(self):
        """Check whether parent sex is consistent."""
        logger.info("Checking for parent sex consistency...")
        ok = True

        name_to_sex = {}
        fathers = set()
        father_of = {}
        mothers = set()
        mother_of = {}

        for donor in self.sheet.donors:
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
                logger.warn(
                    "Donor %s is father of %s but sex is % and not male",
                    name,
                    ", ".join(sorted(father_of[name])),
                    sex,
                )
                ok = False
        for name in mothers:
            sex = name_to_sex.get(name, "unknown")
            if sex != "female":
                logger.warn(
                    "Donor %s is mother of %s but sex is % and not female",
                    name,
                    ", ".join(sorted(mother_of[name])),
                    sex,
                )
                ok = False

        return ok

    def _check_dangling_parents(self):
        """Check whether there are any dangling parents."""
        logger.info("Checking for dangling parents...")
        ok = True

        donor_names = {donor.name for donor in self.sheet.donors}
        for donor in self.sheet.donors:
            if donor.father and donor.father.name not in donor_names:
                logger.warn("Father of %s is not known: %s", donor.father.name, donor.name)
                ok = False
            if donor.mother and donor.mother.name not in donor_names:
                logger.warn("Mother of %s is not known: %s", donor.father.name, donor.name)
                ok = False

        return ok

    def _check_family_id(self):
        """Check whether parents links point over family boundaries."""
        ok = True

        seen_family_ids = {}

        for pedigree in self.sheet.cohort.pedigrees:
            if pedigree.index.extra_infos.get("familyId"):
                if pedigree.index.extra_infos.get("familyId") in seen_family_ids:
                    # TODO: in the future this will be OK once we do not need linking fake entries for snappy any more.
                    logger.warn("Family seen for two unconnected pedigrees")
                    ok = False
            family_ids = {donor.extra_infos.get("familyId") for donor in pedigree.donors}
            if len(family_ids) != 1:
                logger.warn(
                    "Seen multiple family IDs within one pedigree: %s",
                    ", ".join(sorted(family_ids)),
                )
                ok = False

        no_family_donors = {
            donor for donor in self.sheet.donors if not donor.extra_infos.get("familyId")
        }
        if no_family_donors:
            logger.warn(
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

    def __init__(self, sheet: shortcuts.GermlineCaseSheet, base_dir: str):
        #: Germline shortcut sheet.
        self.sheet = sheet
        #: Base directory to search steps in.
        self.base_dir = base_dir

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

    def check_file(self, ped_path):
        results = []

        with open(ped_path, "rt") as pedf:
            donors = list(parse_ped.parse_ped(pedf))
            if not donors:
                logger.error("Empty pedigree file %s", ped_path)
                results.append(False)
                return False
            else:
                ped_names = [donor.name for donor in donors]
                pedigree = self.sheet.donor_ngs_library_to_pedigree.get(donors[0].name)
                if not pedigree:
                    logger.error("Could not find pedigree for %s (%s)", donors[0].name, ped_path)
                    results.append(False)
                    return False
                pedigree_names = [
                    donor.dna_ngs_library.name for donor in pedigree.donors if donor.dna_ngs_library
                ]
                if set(ped_names) != set(pedigree_names):
                    logger.error(
                        "Inconsistency between PED members (%s) and pedigree members (%s) in PED file: %s",
                        ", ".join(ped_names),
                        ", ".join(pedigree_names),
                        ped_path,
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
        sheet_pedigree = self.sheet.donor_ngs_library_to_pedigree.get(ped_donor.name)
        if not sheet_pedigree:
            logger.error(
                "Could not find sheet pedigree for PED donor %s in %s", ped_donor.name, ped_path
            )
            return None
        for tmp in sheet_pedigree.donors:
            if tmp.dna_ngs_library and tmp.dna_ngs_library.name == ped_donor.name:
                return tmp
        else:  # if not break-out
            logger.error(
                "Could not find sheet donor for PED donor %s in %s", ped_donor.name, ped_path
            )
            return None

    def _check_parent(self, ped_donor, sheet_donor, key, ped_path):
        if (getattr(sheet_donor, key) is None) != (getattr(ped_donor, "%s_name" % key) == "0"):
            logger.error(
                "Inconsistency between key of sheet donor and PED donor: %s in %s",
                key,
                ped_donor.name,
                ped_path,
            )
            return False
        elif getattr(sheet_donor, key):
            if not getattr(sheet_donor, key).dna_ngs_library:
                logger.error(
                    "Sheet donor's %s does not have a library: %s", key, sheet_donor.name, ped_path
                )
                return False
            elif getattr(sheet_donor, key).dna_ngs_library.name != getattr(
                ped_donor, "%s_name" % key
            ):
                logger.error(
                    "Inconsistent %s name %s vs %s for sheet vs. PED donor %s in %s",
                    key,
                    getattr(sheet_donor, key).dna_ngs_library.name,
                    getattr(ped_donor, "%s_name" % key),
                    ped_donor.name,
                    ped_path,
                )
                return False
        else:
            return True

    def _check_sex_disease(self, ped_donor, sheet_donor, key, ped_path):
        key2 = {"sex": "sex", "disease": "isAffected"}[key]
        if getattr(ped_donor, key) != sheet_donor.extra_infos.get(key2, "unknown"):
            logger.error(
                "Inconsistent %s between PED and sheet donor %s: %s vs %s in %s",
                key,
                ped_donor.name,
                getattr(ped_donor, key),
                sheet_donor.extra_infos.get(key, "unknown"),
                ped_path,
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

    def check_file(self, vcf_path):
        real_path = os.path.realpath(vcf_path)
        if not os.path.exists(real_path):
            logger.error(
                "Symlink problem: %s points to %s but that does not exist", vcf_path, real_path
            )
            return False
        with warnings.catch_warnings():  # suppress warnings
            warnings.simplefilter("ignore")
            with vcfpy.Reader.from_path(vcf_path) as reader:
                vcf_names = reader.header.samples.names
                if not vcf_names:
                    logger.error("Found no samples in VCF path %s", vcf_path)
                    return False
                pedigree = self.sheet.donor_ngs_library_to_pedigree.get(vcf_names[0])
                if not pedigree:
                    logger.error("Could not find pedigree for %s (%s)", vcf_names[0], vcf_path)
                    return False
                pedigree_names = [
                    donor.dna_ngs_library.name for donor in pedigree.donors if donor.dna_ngs_library
                ]
                if set(vcf_names) != set(pedigree_names):
                    logger.error(
                        "Inconsistency between VCF members (%s) and pedigree members (%s) in VCF file: %s",
                        ", ".join(vcf_names),
                        ", ".join(pedigree_names),
                        vcf_path,
                    )
                    return False
        return True


class SnappyCheckCommand:
    """Implementation of the ``check`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        #: Raw sample sheet.
        self.sheet = load_sheet_tsv(self.args)
        #: Shortcut sample sheet.
        self.shortcut_sheet = shortcuts.GermlineCaseSheet(self.sheet)

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
            default=os.getcwd(),
            required=False,
            help="Base path of project (contains 'ngs_mapping/' etc.), defaults to current path.",
        )

        parser.add_argument(
            "biomedsheet_tsv",
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

        if not os.path.exists(args.base_path):  # pragma: nocover
            logger.error("Base path %s does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-sak snappy check")
        logger.info("  args: %s", self.args)

        results = [
            GermlineSheetChecker(self.shortcut_sheet).run_checks(),
            PedFileCheck(self.shortcut_sheet, self.args.base_path).run_checks(),
            VcfFileCheck(self.shortcut_sheet, self.args.base_path).run_checks(),
        ]

        logger.info("All done")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-sak snappy itransfer-raw-data``."""
    return SnappyCheckCommand.setup_argparse(parser)
