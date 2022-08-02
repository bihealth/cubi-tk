"""Common code to parse BioMedSheets"""
from logzero import logger


class ParseSampleSheet:
    """Class contains methods to parse BioMedSheet"""

    #: Whether to look into the largest start batch in family.
    start_batch_in_family: bool = False

    @staticmethod
    def _build_family_max_batch(sheet, batch_key, family_key):
        family_max_batch = {}
        for donor in sheet.bio_entities.values():
            if batch_key in donor.extra_infos and family_key in donor.extra_infos:
                family_id = donor.extra_infos[family_key]
                batch_no = donor.extra_infos[batch_key]
                family_max_batch[family_id] = max(family_max_batch.get(family_id, 0), batch_no)
        return family_max_batch

    def _batch_of(self, donor, family_max_batch, batch_key, family_key):
        if batch_key in donor.extra_infos:
            batch = donor.extra_infos[batch_key]
        else:
            batch = 0
        if self.start_batch_in_family and family_key in donor.extra_infos:
            family_id = donor.extra_infos[family_key]
            batch = max(batch, family_max_batch.get(family_id, 0))
        return batch

    def yield_ngs_library_names(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all NGS library names from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            for bio_sample in donor.bio_samples.values():
                for test_sample in bio_sample.test_samples.values():
                    for library in test_sample.ngs_libraries.values():
                        yield library.name

    def yield_sample_names(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield all sample names (``secondary_id``) from sheet.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        for donor in self.yield_donor(sheet, min_batch, max_batch, batch_key, family_key):
            yield donor.secondary_id

    def yield_donor(
        self, sheet, min_batch=None, max_batch=None, batch_key="batchNo", family_key="familyId"
    ):
        """Yield donor object from sheet.

        When ``min_batch`` is given then only the donors for which the ``extra_infos[batch_key]`` is greater than
        ``min_batch`` will be used.

        :param sheet: Sample sheet.
        :type sheet: biomedsheets.models.Sheet

        :param min_batch: Minimum batch number to be extracted from the sheet. All samples in batches below the
        threshold will be skipped.
        :type min_batch: int

        :param max_batch: Maximum batch number to be extracted from the sheet. All samples in batches above the
        threshold will be skipped.
        :type max_batch: int

        :param batch_key: Batch number key in sheet. Default: 'batchNo'.
        :type batch_key: str

        :param family_key: Family identifier key. Default: 'familyId'.
        :type family_key: str
        """
        family_max_batch = self._build_family_max_batch(sheet, batch_key, family_key)

        # Process all libraries and filter by family batch ID.
        for donor in sheet.bio_entities.values():
            # Ignore below min batch number if applicable
            if min_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch < min_batch:
                    logger.debug(
                        "Skipping donor %s because %s = %d < min_batch = %d",
                        donor.name,
                        batch_key,
                        batch,
                        min_batch,
                    )
                    continue
            # Ignore above max batch number if applicable
            if max_batch is not None:
                batch = self._batch_of(donor, family_max_batch, batch_key, family_key)
                if batch > max_batch:
                    logger.debug(
                        "Skipping donor %s because %s = %d > max_batch = %d",
                        donor.name,
                        batch_key,
                        batch,
                        max_batch,
                    )
                    # It would be tempting to add a `break`, but there is no guarantee that
                    # the sample sheet is sorted.
                    continue
            yield donor
