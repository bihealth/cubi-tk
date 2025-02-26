"""``cubi-tk snappy check-local``: check within sample sheet and between sample sheet and files."""

import argparse
import glob
import os
import pathlib
import typing
import warnings

from biomedsheets import shortcuts
from cubi_tk.exceptions import ParameterException
from loguru import logger
import vcfpy

from .. import parse_ped
from .common import get_all_biomedsheet_paths, get_biomedsheet_path, load_sheet_tsv


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

    @staticmethod
    def _check_parent_sex(sheet: shortcuts.GermlineCaseSheet):
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
                father_of_str = ", ".join(sorted(father_of[name]))
                logger.warning(
                    f"Donor '{name}' is father of '{father_of_str}' but sex is '{sex}' and not male"
                )
                ok = False
        for name in mothers:
            sex = name_to_sex.get(name, "unknown")
            if sex != "female":
                mother_of_str = ", ".join(sorted(mother_of[name]))
                logger.warning(
                    f"Donor '{name}' is mother of '{mother_of_str}' but sex is '{sex}' and not female"
                )
                ok = False

        return ok

    @staticmethod
    def _check_dangling_parents(sheet: shortcuts.GermlineCaseSheet):
        """Check whether there are any dangling parents."""
        logger.info("Checking for dangling parents...")
        ok = True

        donor_names = {donor.name for donor in sheet.donors}
        for donor in sheet.donors:
            if donor.father and donor.father.name not in donor_names:
                logger.warning(f"Father of '{donor.father.name}' is not known: {donor.name}")
                ok = False
            if donor.mother and donor.mother.name not in donor_names:
                logger.warning(f"Mother of '{donor.mother.name}' is not known: {donor.name}")
                ok = False

        return ok

    @staticmethod
    def _check_family_id(sheet: shortcuts.GermlineCaseSheet):
        """Check whether parents links point over family boundaries."""
        logger.info("Checking for family identifiers...")

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
                family_ids_str = ", ".join(sorted(family_ids))
                logger.warning(f"Seen multiple family IDs within one pedigree: {family_ids_str}")
                ok = False

        no_family_donors = {
            donor for donor in sheet.donors if not donor.extra_infos.get("familyId")
        }
        if no_family_donors:
            no_family_donors_str = ", ".join(sorted([donor.name for donor in no_family_donors]))
            logger.warning(f"Found donors without family ID: {no_family_donors_str}")
            ok = False

        return ok
    
class CancerSheetChecker:
    """Helper class that implements the consistency checks within cancer sheets."""

    def __init__(self, sheets: typing.Iterable[shortcuts.CancerCaseSheet]):
        #: Shortcut sheet.
        self.sheets = list(sheets)

    def run_checks(self):
        """Execute checks, return True if all good else False."""
        logger.info("Running cancer sheet checks...")
        results = []
        for sheet in self.sheets:
            results += [
                #self._check_patient_sex(sheet),
                self._check_tumor_and_normal(sheet),
            ]
        return all(results)

    @staticmethod
    def _check_patient_sex(sheet: shortcuts.CancerCaseSheet):
        """Check whether sex is consistent."""
        #TODO: add sex to cancer biomedsheet and shortcut functions
        logger.info("Checking for consistency of sex for patient samples (if sex exists)..")
        ok = True
        return ok

    @staticmethod
    def _check_tumor_and_normal(sheet: shortcuts.CancerCaseSheet):
        """Check whether there are tumor and normal samples."""
         #TODO: Maybe add allow_missing_tumor and allow_missing_normal to argpars and use here if needed
        logger.info("Checking for at least one tumor and normal sample per patient...")
        ok = True
        for donor in sheet.donors:
            has_normal = False
            has_tumor = False
            for sample in donor.bio_samples:
                if sample.startswith("N"):
                    has_normal = True
                elif sample.startswith("T"):
                    has_tumor = True
            if(not has_normal):
                logger.warning("Patient {} is missing normal sample", donor.bio_entity.name)
            if(not has_tumor):
                logger.warning("Patient {} is missing tumor sample", donor.bio_entity.name)
            ok = ok and has_normal and has_tumor
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
            logger.debug(f"Checking {file_path}")
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
                logger.error("Empty pedigree file {}", ped_path)
                results.append(False)
                return False
            else:
                ped_names = {donor.name for donor in donors}
                pedigree = self.donor_ngs_library_to_pedigree.get(donors[0].name)
                if not pedigree:
                    logger.error(
                        (
                            "Found pedigree in PED file but not in sample sheet. \n\n"
                            "index:    {}\n"
                            "PED file: {}\n"
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
                            "shared:     {}\n"
                            "sheet only: {}\n"
                            "PED only:   {}\n"
                            "PED file:   {}\n"
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
                "Found no pedigree in sample sheet for PED donor.\n\nPED donor: {}\nPED file:  {}\n",
                ped_donor.name,
                os.path.relpath(ped_path, self.base_dir),
            )
            return None
        for tmp in sheet_pedigree.donors:
            if tmp.dna_ngs_library and tmp.dna_ngs_library.name == ped_donor.name:
                return tmp
        logger.error(
            "Member in PED not found in sample sheet.\n\nPED donor: {}\nPED file:  {}\n",
            ped_donor.name,
            os.path.relpath(ped_path, self.base_dir),
        )
        return None

    def _check_parent(self, ped_donor, sheet_donor, key, ped_path):
        if (getattr(sheet_donor, key) is None) != (getattr(ped_donor, "%s_name" % key) == "0"):
            logger.error(
                "Inconsistent {} between PED and sample sheet.\n\ndonor:    {}\nPED file: {}\n",
                key,
                ped_donor.name,
                os.path.relpath(ped_path, self.base_dir),
            )
            return False
        elif getattr(sheet_donor, key):
            if not getattr(sheet_donor, key).dna_ngs_library:
                logger.error(
                    "Sheet donor's {} does not have library.\n\ndonor:    {}\nPED file: {}\n",
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
                        "Inconsistent {} name between PED and sample sheet.\n\n"
                        "donor:    {}\n"
                        "in sheet: {}\n"
                        "in PED:   {}\n"
                        "PED file: {}\n"
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
                    "Inconsistent {} between PED and sample sheet.\n\n"
                    "donor:    {}\n"
                    "in sheet: {}\n"
                    "in PED:   {}\n"
                    "PED file: {}\n"
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
                f"Symlink problem, points to non-existing path\n\n"
                f"link: {os.path.relpath(vcf_path, self.base_dir)}\n"
                f"dest: {os.path.relpath(real_path, self.base_dir)}\n"
            )
            return False
        with warnings.catch_warnings():  # suppress warnings
            warnings.simplefilter("ignore")
            with vcfpy.Reader.from_path(vcf_path) as reader:
                vcf_names = reader.header.samples.names
                if not vcf_names:
                    logger.error(
                        f"Found no samples in VCF path\n\nVCF path: {os.path.relpath(vcf_path, self.base_dir)}"
                    )
                    return False
                pedigree = self.donor_ngs_library_to_pedigree.get(vcf_names[0])
                if not pedigree:
                    logger.error(
                        f"Index from VCF not found in sample sheet.\n\n"
                        f"index:    {vcf_names[0]}\n"
                        f"VCF path: {os.path.relpath(vcf_path, self.base_dir)}\n"
                    )
                    return False
                vcf_names = set(vcf_names)
                pedigree_names = {
                    donor.dna_ngs_library.name for donor in pedigree.donors if donor.dna_ngs_library
                }
                if vcf_names != pedigree_names:
                    logger.error(
                        (
                            f"Inconsistent members between VCF and sample sheets.\n\n"
                            f"shared:     {', '.join(sorted(vcf_names & pedigree_names)) or '(none)'}\n"
                            f"VCF only:   {', '.join(sorted(vcf_names - pedigree_names)) or '(none)'}\n"
                            f"sheet only: {', '.join(sorted(pedigree_names - vcf_names)) or '(none)'}\n"
                            f"VCF path:   { os.path.relpath(vcf_path, self.base_dir)}\n"
                        )
                    )
                    return False
        return True


class SnappyCheckLocalCommand:
    """Implementation of the ``check-local`` command."""

    def __init__(self, args):
        #: Command line arguments.
        self.args = args
        # Find biomedsheet file
        self.biomedsheet_tsvs = None
        if self.args.project_uuids:
            self.biomedsheet_tsvs = [
                get_biomedsheet_path(start_path=self.args.base_path, uuid=uuid)
                for uuid in self.args.project_uuids
            ]
        else:
            self.biomedsheet_tsvs = get_all_biomedsheet_paths(start_path=self.args.base_path)
        #: Raw sample sheet.
        self.sheets = [load_sheet_tsv(tsv, args.tsv_shortcut) for tsv in self.biomedsheet_tsvs]
        #: Shortcut sample sheet.
        if self.args.tsv_shortcut == "germline":
            self.shortcut_sheets = [shortcuts.GermlineCaseSheet(sheet) for sheet in self.sheets]
        elif self.args.tsv_shortcut == "cancer":
            options = shortcuts.CancerCaseSheetOptions(allow_missing_normal=True, allow_missing_tumor=True)
            self.shortcut_sheets = [shortcuts.CancerCaseSheet(sheet, options= options) for sheet in self.sheets]
        else:
            raise ParameterException("tsv shortcut not supported, valid values are 'cancer' and 'germline'")
         


    @classmethod
    def setup_argparse(cls, parser: argparse.ArgumentParser) -> None:
        """Setup arguments for ``check-local`` command."""
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
            help=(
                "Base path of project (contains 'ngs_mapping/' etc.), spiders up from biomedsheet_tsv and falls "
                "back to current working directory by default."
            ),
        )
        parser.add_argument(
            "project_uuids",
            type=str,
            nargs="*",
            help="UUID(s) from project(s) to check. Use all if not given.",
        )

    @classmethod
    def run(
        cls, args, _parser: argparse.ArgumentParser, _subparser: argparse.ArgumentParser
    ) -> typing.Optional[int]:
        """Entry point into the command."""
        return cls(args).execute()

    def check_args(self, args):
        """Called for checking arguments."""
        res = 0

        for tsv_file in self.biomedsheet_tsvs:
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
            logger.error("Base path {} does not exist", args.base_path)
            res = 1

        return res

    def execute(self) -> typing.Optional[int]:
        """Execute the transfer."""
        res = self.check_args(self.args)
        if res:  # pragma: nocover
            return res

        logger.info("Starting cubi-tk snappy check-local")
        logger.info("  args: {}", self.args)
        if self.args.tsv_shortcut == "germline":
            results = [
            GermlineSheetChecker(self.shortcut_sheets).run_checks(),
            PedFileCheck(self.shortcut_sheets, self.args.base_path).run_checks(),
            VcfFileCheck(self.shortcut_sheets, self.args.base_path).run_checks(),
            ]
        elif self.args.tsv_shortcut == "cancer":
            results = [
            CancerSheetChecker(self.shortcut_sheets).run_checks(),
            #TODO: VcfFileCheck if needed
            ]
        else:
            raise ParameterException("tsv shortcut not supported, valid values are 'cancer' and 'germline'")

        logger.info("All done")
        return int(not all(results))


def setup_argparse(parser: argparse.ArgumentParser) -> None:
    """Setup argument parser for ``cubi-tk snappy check-local``."""
    return SnappyCheckLocalCommand.setup_argparse(parser)
