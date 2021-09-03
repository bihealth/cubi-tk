.. _man_dkfz:

===================
Manual for ``dkfz``
===================

The ``cubi-tk dkfz`` provides you commands to facilitate the upload of datasets sequenced in the DKFZ center in Heidelberg.
This document provides an overview of these commands, and how they can be adapted to meet specific needs.

-----------------------------------------------
Background: what is in the data from Heidelberg
-----------------------------------------------

The data is downloaded from DKFZ's mid-term storage server is organised in the following way::

    downloaded_from_DKFZ/
    └── 22404                                                       # ILSe run ID, always a number
        └── data
            ├── 210701_ST-K00207_0361_AHLG7TBBXY                    # Sequencing run ID
            │   ├── 210701_ST-K00207_0361_AHLG7TBBXY_meta.tsv       # Metafile for the sequencing run
            │   └── AS-644878-LR-57198                              # Sample & library id
            │       └── fastq
            │           ├── AS-644878-LR-57198_R1_fastqc.html       # fastqc output (unused & unsaved)
            │           ├── AS-644878-LR-57198_R1_fastqc.zip        # idem
            │           ├── AS-644878-LR-57198_R1.fastq.gz          # Fastq for 1st read mate
            │           └── AS-644878-LR-57198_R1.fastq.gz.md5sum   # md5 checksum
            ├── 22404_meta.tsv                                      # Metafile for the ILSe run
            └── 22404_report.pdf                                    # Report in human-readable form


(there may be more than one sample & library directory in one sequencing run, and more than one sequencing run per ILSe run, possibly mixing whole exome sequencing and mRNA sequencing, for example).

Unlike in SODAR, the downloaded files are organised and named according to the sequencing. The meta file ``<ILSE_nb>_meta.tsv`` provides the mapping between files and samples. It contains information on:

- files: filename, md5 checksum, mate, lane, ...
- sequencing: platform, instrument type, sequencing kit, index, ...
- sample: sample id, gender, tissue type, ...
- base calling: software version, phred score, QC measures, ...

**The purpose of the module is:**

- to process this metafile to extract information important for SODAR,
- to transform sample & library identifiers to a format compatible with SODAR & snappy requirements,
- to create isatab files (assay, sample & investigation) that contain the information provided in the metafile, and
- to upload the fastq files and meta files to SODAR.


-----------
Basic usage
-----------

**Important caveat:** By default, the parsing of metafile(s), and in particular the creation of ids for the source, sample, extract and library assumes the conventions adopted for the DKTK Master programme. It is likely to fail for other naming conventions. However, the parser & id mappuing steps are configurable using yaml files, see below.

**ISATAB files creation:**

.. code-block:: bash

    $ cubi-tk dkfz prepare-isatab METAFILE [METAFILE ...] DESTINATION

Unlike other ``cubi-tk`` commands, here ``DESTINATION`` is not a landing zone, but a local directory where the created isatab files will be stored.

Note that for a proper numbering of the samples, it is important to include the meta files of all ILSe runs that belong the the study.

**fastq upload:**

.. code-block:: bash

    $ cubi-tk dkfz ingest-fastq METAFILE [METAFILE ...] DESTINATION

``DESTINATION`` is either an iRODS path to a *landing zone* in SODAR or the UUID of that *landing zone*.

**metafile(s) upload:**

.. code-block:: bash

    $ cubi-tk dkfz ingest-meta METAFILE [METAFILE ...] DESTINATION

is used to upload the metafile(s), the pdf report(s) and the table of id mappings to SODAR. They are uploaded to landing zone ``DESTINATION``, but are eventually store in the project's ``MiscFiles/DKFZ_meta``, except for the ids mapping table which is saved under ``MiscFiles/DKFZ_upload/<date>``


--------------------
Parser configuration
--------------------

The metafile is a tab-delimited text file, with one row per fastq file. During the years, the column title have slightly changed, new columns have appeared and others disappeared. To ensure that the parser does not become obsolete when the metafile format changes again, a yaml configuration file is used to define the details of how information is extracted.

The parser configuration file is found in the ``cubi-tk/isa_tpl/isatab-dkfz/DkfzMetaParser.yaml`` sub-directory of the cubi-tk distribution. It is divided into two parts: the first describes how ontologies & study assays are added to the ISATAB investigation file. The second, which is discussed here, defines which metafile information is used & stored.

The parser tries to follow ISATAB concepts: the metadata information items are attached to materials or processes, as shown in the examples below:

.. code-block:: yaml

    Material:
        - type: Source Name
          meta_columns: ["PATIENT_ID"]
          characteristics:
              - name: Sex
                meta_columns: ["SEX", "GENDER"]
              - name: Organism
                meta_columns: ["SPECIES"]
                processor: get_organism
          comments: []

The code above that the name of ``Source`` materials will be taken from the ``PATIENT_ID`` column of the metafile. A ``Sex`` characteristic will be attached to the material, and filled with the contents of the ``SEX`` or ``GENDER`` columns (the name of this column changed with the metadata format releases). The ``Organism`` characteristic is taken from the ``SPECIES`` column, post-processed by the ``get_organism`` method of the parser. No comments columns are attached to the source.

.. code-block:: yaml

    Process:
        - type: nucleic acid sequencing
          add_assay_type: yes
          date: ["RUN_DATE"]
          performer: ~
          parameters:
              - name: Instrument Model
                meta_columns: ["INSTRUMENT_MODEL"]
                processor: get_instrument_model
              - name: Platform
                meta_columns: ["INSTRUMENT_PLATFORM", "PLATFORM"]
              - name: Sequencing kit
                meta_columns: ["SEQUENCING_KIT"]
              - name: Center Name
                fixed_value: DKFZ Heidelberg
          comments: []

The ``nucleic acid sequencing`` process must be qualified with the type of assay (``RNA``, ``EXON`` or ``WGS``), and it is achieved by setting ``add_assay_type: yes``. The date is taken from column ``RUN_DATE``, and the performer is kept empty. The process's ``Instrument Model`` parameter is post-processed by the method ``get_instrument_model``, and the ``Center Name`` parameter is set to constant value ``DKFZ Heidelberg``.

Finally, the logical connections between materials & processes is described in the ``Arc`` section. The source (typically the patient) is at the origin of the workflow. The sample is obtained from the source by the ``sample collection`` process, the ``nucleic acid extraction`` process generates one or more extracts (typically dna or rna material), from which libraries are made by the ``library construction`` process, finally sequenced by the ``nucleic acid sequencing`` process to give data files. Note that other workflows are untested (and unlikely to work).

The ``MD5`` column are used to uniquely identify all files, and the ``RUN_ID`` & ``FASTQ_FILE`` to locate fastq files in the downloaded file directory structure. The assay type is taken from the ``SEQUENCING_TYPE`` column, and the values currently implemented are EXON, RNA & WGS.

To change the parser behaviour, the user must create her own yaml configuration file, and point to it using the ``--parsing-config`` option of the command (valid for all ``dkfz`` sub-commands).

--------------------
Sample & library ids
--------------------

As for the parser, the id creation step can be configured using the ``--mapping-config`` option (the default mapping configuration is in ``cubi-tk/isa_tpl/isatab-dkfz/DkfzMetaIdMappings.yaml``. Again, the mapper generates ids for ISATAB materials source, sample, extract & library. The naming of raw data files is unaffected.

In the DKTK Master programme, the ``SAMPLE_ID`` column contains an id defined as ``<project>-<source>-<sample>-<extract>``. Therefore, because it has inforamtion about the extract, the parser saves the id in the ``dkfz_id`` characteristics of the extract material, as shown below.

.. code-block:: yaml

    Material:
        - type: Extract Name
          characteristics:
              - name: dkfz_id
                meta_columns: ["SAMPLE_ID", "SAMPLE_NAME"]
                enforce_present: yes


From that extract id, the CUBI identifiers are generated using the following rules:

.. code-block:: yaml

    Source:
        Material: Extract Name
        characteristic: dkfz_id
        pattern: "^ *([A-z0-9_]+)-([A-z0-9_]+)-([A-Z][0-9]+)-([A-Z][0-9]+)(-[0-9]+)? *$"
        group: 2
    Sample:
        Material: Extract Name
        characteristic: dkfz_id
        pattern: "^ *([A-z0-9_]+)-([A-z0-9_]+)-([A-Z][0-9]+)-([A-Z][0-9]+)(-[0-9]+)? *$"
        group: 3
        replace:
            Material: Sample Name
            characteristic: isTumor
            increment: yes
    Extract:
        Material: Extract Name
        characteristic: dkfz_id
        pattern: "^ *([A-z0-9_]+)-([A-z0-9_]+)-([A-Z][0-9]+)-([A-Z][0-9]+)(-[0-9]+)? *$"
        group: 4
        replace:
            Process: library construction
            parameter: Library source
            increment: yes
            mappings:
                - when: "^GENOMIC$"
                  replacement: "DNA"
                - when: "^TRANSCRIPTOMIC$"
                  replacement: "RNA"
    Library:
        Material: Library Name
        characteristic: Batch
        pattern: "^ *0*([0-9]+) *$"
        group: 1
        replace:
            Process: library construction
            parameter: Library strategy
            increment: yes
            mappings:
                - when: "^WXS$"
                  replacement: "WES"
                - when: "^RNA-Seq$"
                  replacement: "mRNA_seq"
                - when: "^WGS$"
                  replacement: "WGS"


- For the source id, the mapper takes its information from the ``dkfz_id`` of the extract material. The regular expression in ``pattern`` is used to extract the second group of the original id, which is a source identifier suitable for SODAR & snappy.
- for the sample id, the third group is used, but on its own, it is not a good snappy identifier for normal/tumor cancer projects. So instead, it is replaced by the ``isTumor`` characteristic of the sample material (which is N for normal & T for tumor in that project).
- for the extract id, the fourth group is replaced by values selected from the contents of the ``Library source`` parameter of the ``library construction`` process. When the latter is ``GENOMIC``, the extract id will be ``DNA``, when it's ``TRANSCRIPTOMICS``, it will be ``RNA``.
- finally, the library id must be taken from another source, as the original DKFZ id only identifes the extract. The ILSe id stored in parameter ``Batch`` of the ``library construction`` process is used as library identifier. For snappy, however, it must be replaced by values extracted from the ``Library strategy`` parameter.


------------------------------
Changing the id mapping scheme
------------------------------

As an example, we show here the mapping configuration that was used to process different kind of ids in the ``SAMPLE_ID`` column of the metafile. In this case, the id provided by DKFZ was in the form ```PNET<donor number>``, followed by ``N`` for normal samples and ``P<number>`` for tumor samples. The ``dkfz_id`` column was made a characteristic of the sample, rather than of the extract, and the mapper configuration was set to

.. code-block:: yaml

    Source:
        Material: Sample Name
        characteristic: dkfz_id
        pattern: "^ *(PNET[0-9]+)([NP][0-9]*) *$"
        group: 1
    Sample:
        Material: Sample Name
        characteristic: dkfz_id
        pattern: "^ *(PNET[0-9]+)([NP][0-9]*) *$"
        group: 2
        replace:
            Material: Sample Name
            characteristic: dkfz_id
            increment: yes
            mappings:
                - when: "^ *PNET[0-9]+N *$"
                  replacement: "N"
                - when: "^ *PNET[0-9]+P[0-9]+ *$"
                  replacement: "T"
    Extract:
        Material: Sample Name
        characteristic: dkfz_id
        pattern: "^ *(PNET[0-9]+[NP][0-9]*) *$"
        group: 1
        # From this point, identical to the default
        # replace: ...

The rest of the file was unchanged from the default.

----------------
More Information
----------------

Also see ``cubi-tk dkfz --help``, ``cubi-tk dkfz prepare-isatab --help``, ``cubi-tk dkfz ingest-fastq --help`` & ``cubi-tk dkfz ingest-meta --help`` for more information.
