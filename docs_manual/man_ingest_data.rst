.. _man_ingest_data:

=================================
Manual for ``sodar ingest-data``
=================================

The ``cubi-tk sodar ingest-data`` command lets you upload raw data files to SODAR.
It is configured for uploading FASTQ files by default, but the parameters can be adjusted to upload any files.

The basic usage is:

.. code-block:: bash

    $ cubi-tk sodar ingest-data SOURCE [SOURCE ...] DESTINATION

where each ``SOURCE`` is a path to a folder containing relevant files (this can also be a WebDav URL, see below) and
``DESTINATION`` is either an iRODS path to a *landing zone* in SODAR, the UUID of that *landing zone*, or and SODAR project UUID.

-------
Presets
-------

The ``cubi-tk sodar ingest-data`` command comes with several presets for uploading specific files,
used with the ``-p`` option. Each preset includes predefined file inout and output patterns.
The presets include:

- fastq [default]
  fastq.gz and fq.gz files with the typical BCLconvert filenames (i.e. "SampleName_S00_L001_R001_01.fastq.gz")

- ONT
  For bam, pod5, txt, and json files as produced by ONT on-board software.

- digestiflow
  For fastq.gz files as produced by digestiflow

- onk_analysis
  For Dragen out from the oncology workflow.


If you have another common use-case for uploading specific file sets feel free to request or provide a new preset.

---------------------
Custom file selection
---------------------

By default, the parameters ``--src-regex`` and ``--remote-dir-pattern`` are configured for FASTQ files or the chosen preset.
However, they may also be changed to upload any other files as well. The two parameters have the following functions:

- ``--src-regex``: a regular expression to recognize paths to raw data files to upload (the paths starting from the ``SOURCE`` directories).
- ``--remote-dir-pattern``: a pattern specifying into which folder structure the raw data files should be uploaded.
  This is a file path with wildcards that are replaced by the captured content of named groups in the regular expression passed via ``--src-regex``.

For example, the default ``--src-regex`` is

.. code-block:: perl

    (.*/)?(?P<sample>.+?)(?:_(?P<lane>L[0-9]+?))?(?:_(?P<mate>R[0-9]+?))?(?:_(?P<batch>[0-9]+?))?\.f(?:ast)?q\.gz

It can capture a variety of different FASTQ file names and has the named groups ``sample``, ``lane``, ``mate`` and ``batch``.
The default ``--remote-dir-pattern`` is

.. code-block:: bash

    {collection_name}/{date}/{filename}

It contains the wildcard ``{collection_name}``, which represents an irods collection and will be filled with the captured
content of group ``(?P<sample>...)``, potentially modified by a regex (see 'Mapping of file names' below).
Alternatively the irods collection name can be derived by mapping the extracted (and modified) ``(?P<sample>...)``
group to any column of the assay table associated with the LZ or project. In this case the ``{library_name}`` will be
filled with the content of the last material column of the assay table (or ``--collection-column`` if defined).
In addition, the wildcards ``{date}`` and ``{filename}`` can always be used in ``--remote-dir-pattern`` and will be
filled with the current date (or ``--remote-dir-date``) and full filename (the basename of a matched file), respectively.

--------------------------------
Changing/Mapping of sample names
--------------------------------

In some cases additional mapping of filenames is required (for example to fully match the irods collections).
This can be done via the parameter ``--sample-collection-mapping`` or short ``-m``.
It can be supplied several times, each time for another mapping.
With each ``-m MATCH REPL`` a pair of a regular expression and a replacement string are specified.
Internally, pythons ``re.sub`` command is executed on the extracted ``(?P<sample>...)`` capture group.
Therefore, you can refer to the documentation of the `re package <https://docs.python.org/3/library/re.html>`_ for syntax questions.

If the file names (or rather file paths) do not contain the necessary information to extract the sodar sample collection
names, then ``cubi-tk sodar ingest-data`` can also map the extracted sample names and first map them against another
column from the Sodar samplesheet to derive the matching collection names.
The ``--match-column`` option is used for this, and needs to be given a column name from the sodar samplesheet (either study
or assay sheet). The ``-m`` glag can be combined with this option and will be used on the file extracted names first.

----------------
More Information
----------------

Also see ``cubi-tk sodar ingest-data`` :ref:`CLI documentation <cli>` and ``cubi-tk sodar ingest-data --help`` for more information.
