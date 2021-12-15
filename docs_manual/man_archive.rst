.. _man_archive:

======================
Manual for ``archive``
======================

The ``cubi-tk archive`` is designed to facilitate the archival of older projects away from the cluster's fast file system.
This document provides an overview of these commands, and how they can be adapted to meet specific needs.

--------
Glossary
--------

Hot storage: Fast and expensive, therefore usually size restricted.
Examples:
- GPFS by DDN (currently at ``/fast``)
- Ceph with SSDs

Warm storage: Slower, but with more space and possibly mirroring.
Examples:
- SODAR with irods
- Ceph with HDDs (``/data/ceph-1/``)

Cold storage: For data that needs to be accessed only rarely.
Examples:
- Tape archive

---------------------------------
Background: the archiving process
---------------------------------

CUBI archive resources are three-fold:

- SODAR and associated irods storage should contain raw data generated for the project. SODAR also contains important results (mapping, variants, differential expression, ...).
- Gitlab contains small files required to generate the results, typically scripts, configuration files, READMEs, meeting notes, ..., but also knock-in gene sequence, list of papers, gene lists, etc.
- The rest should be stored in CEPH (warm storage).

For older projects or intermediate results produced by older pipelines the effort of uploading the data to SODAR & gitlab may not be warranted. In this case, the bulk of the archive might be stored in the CEPH file system.

**The module aims to facilitate this last step, i.e. the archival of old projects to move them away from the hot storage.**

------------------------------
Archiving process requirements
------------------------------

Archived projects should contain all **important** files, but not data already stored elsewhere. In particular, the following files should **not** be archived:

- raw data (``*.fastq.gz`` files) saved in SODAR or in the ``STORE``,
- data from public repositories (SRA, GDC portal, ...) that can easily be downloaded again,
- static data such as genome sequence & annotations, variant databases from gnomAD, ... that can also be easily retrieved,
- indices files for mapping that can be re-generated.

**Importantly, a README file should be present in the archive, briefly describing the project, listing contacts to the client & within CUBI and providing links to SODAR & Gitlab when appropriate.**


**The purpose of the module is:**

- to provide a summary of files that require special attention, for example symlinks which targets lie outside of the project, or large files (`*.fastq.gz` or `*.bam` especially)
- to create a temporary directory that mimicks the archived files with symlinks,
- to use this temporary directory as template to copy files on the CEPH filesystem, and
- to compute checksums on the originals and copies, to ensure accuracy of the copy process.


-----------
Basic usage
-----------

**Summary of files in project**

.. code-block:: bash

    $ cubi-tk archive summary PROJECT_DIRECTORY DESTINATION

Unlike other ``cubi-tk`` commands, here ``DESTINATION`` is not a landing zone, but a local filename for the summary of files that require attention.

By default, the summary reports:

- dangling symlinks (also dangling because of permission),
- symlinks pointing outside of the project directory,
- large (greater than 256MB)  ``*.fastq.gz``, ``*.fq.gz`` & ``*.bam`` files,
- large static data files with extension ``*.gtf``, ``*.gff``, ``*.fasta`` & ``*.fa`` (possibly gzipped), that can potentially be publicly available.
- large files from SRA with prefix ``SRR``.

The summary file is a table with the following columns:

- **Class**: the name(s) of the pattern(s) that match the file. When the file matches several patterns, all are listed, separated by ``|``.
- **Filename**: the relative path of the file (from the project's root).
- **Target**: the symlink's target (when applicable)
- **ResolvedName**: the resolved (absolute, symlinks removed) path of the target. When the target doesn't exist or is inaccessible because of permissions, the likely path of the target.
- **Size**: file size (target file size for symlinks). When the file doesn't exist, it is set to 0.
- **Dangling**: ``True`` when the file cannot be read (missing or inaccessible), ``False`` otherwise.
- **Outside**: ``True`` when the target path is outside of the project directory, ``False`` otherwise. It is always ``False`` for real files (_i.e._ not symlinks).

**Archive preparation: temporary copy**

.. code-block:: bash

    $ cubi-tk archive prepare PROJECT_DIRECTORY TEMPORARY_DESTINATION

``TEMPORARY_DESTINATION`` is here the path to the temporary directory that will be created. It must not exist.

For each file that must be archived, the module creates a symlink to that file's absolute path. The module also reproduces the project's directories hierarchy, so that the symlink sits in the same relative position in the temporary directory than in the original project.

The module deals with symlinks in the project differently whether their target in inside the project or not. For symlinks pointing outside of the project, a symlink to the target's absolute path is created. For symlinks pointing inside the project, a relative path symlink is created. This allows to store all files (even those outside of the project), without duplicating symlinks inside the project.

Additional transformation of the original files are carried out during the preparation step:

- The contents of the ``.snakemake``, ``sge_log``, ``cubi-wrappers`` & ``snappy-pipeline`` directories are processed differently: the directories are tarred & compressed in the temporary destination, to reduce the number of inodes in the archive.
- The core dump files are not copied to the temporary destination, and therefore won't be copied to the final archive.
- A ``README.md`` file is also created by the module, if there isn't one already which contains contact information. Upon creation, the module prompts the user for values that will populate ``REAMDE.md``. These values can also be included on the command line.

**Copy to archive & verification**

.. code-block:: bash

    $ cubi-tk archive copy TEMPORARY_DESTINATION FINAL_DESTINATION

``FINAL_DESTINATION`` is here the path to the final destination of the archive, on the warm storage. It must not exist.



-------------
Configuration
-------------

The files reported in the summary are under user control, through the ``--classes`` option, which must point to a yaml file describing the regular expression pattern & minimum size for each class. For example, raw data files can be identified as follows:

.. code-block:: yaml

    fastq:
        min_size: 268435456
        pattern: "^(.*/)?[^/]+(\\.f(ast)?q(\\.gz)?)$"


The files larger than 256MB, with extension ``*.fastq``, ``*.fq``, ``*.fastq.gz`` or ``*.fq.gz`` will be reported with the class ``fastq``.
Any number of file class can be defined. The default classes configuration is in ``cubi-tk/isa_tpl/archive/classes.yaml``

The behaviour of the archive preparation can also be changed using the ``--rules`` option. The rules are also described in a yaml file by regular expression patterns.

Three different archiving options are implemented:

- **ignore**: the files or directories matching the pattern are simply omitted from the temporary destination. This is useful to ignore remaining temporary files, core dumps or directories containing lists of input symlinks, for example.
- **compress**: the files or directories matching the pattern will be replaced in the temporary destination by a compressed (gzipped) tar file. This is how ``.snakemake`` or ``sge_log`` directories are treated by default, but patterns for other directories may be added, for example for the Slurm log directories.
- **squash**: the files matching the pattern will be replaced by zero-length placeholders in the temporary destination. A md5 checksum file will be added next to the original file, to enable verification.


--------
Examples
--------

Consider an example project. It contains:

- raw data in a ``raw_data`` directory, some of which is stored outside of the project's directory,
- processing results in the ``pipeline`` directory, 
- additional data files & scripts in ``extra_data``,
- a ``.snakemake`` directory that can potentially contain many files in conda environments, for example, and
- a bunch on temporary & obsolete files that shouldn't be archived, conveniently grouped into the ``ignored_dir`` directory.

The architecture of this toy project is displayed below::


    project/
    ├── extra_data
    │   ├── dangling_symlink -> ../../outside/inexistent_data
    │   ├── file.public
    │   ├── to_ignored_dir -> ../ignored_dir
    │   └── to_ignored_file -> ../ignored_dir/ignored_file
    ├── ignored_dir
    │   └── ignored_file
    ├── pipeline
    │   ├── output
    │   │   ├── sample1
    │   │   │   └── results -> ../../work/sample1/results
    │   │   └── sample2 -> ../work/sample2
    │   └── work
    │       ├── sample1
    │       │   └── results
    │       └── sample2
    │           └── results
    ├── raw_data
    │   ├── batch1 -> ../../outside/batch1
    │   ├── batch2
    │   │   ├── sample2.fastq.gz -> ../../../outside/batch2/sample2.fastq.gz
    │   │   └── sample2.fastq.gz.md5 -> ../../../outside/batch2/sample2.fastq.gz.md5
    │   └── batch3
    │       ├── sample3.fastq.gz
    │       └── sample3.fastq.gz.md5
    └── .snakemake
        └── snakemake


Prepare the copy on the temporary destination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Imagine now that the raw data is already safely archived in SODAR. We don't want to save these files in duplicate, so we decide ito _squash_ the raw data files so that their size is set to 0, and their md5 checksum is added. We also do the same for the publicly downloadable file ``file.public``. We also want to ignore the junk in ``ignored_dir``, and to compress the ``.snakemake`` directory. So we have the following rules:


.. code-block: yaml

    ignore:
        - ignored_dir

    compress:
        - "^(.*/)?\\.snakemake$"

    squash:
        - "^(.*/)?file\\.public$"
        - "^(.*/)?raw_data/(.*/)?[^/]+\\.fastq\\.gz$"


After running the preparation command ``cubi-tk archive prepare --rules my_rules.yaml project temp_dest``, the temporary destination contains the following files::

    tests/data/archive/temp_dest
    ├── <today's date>_hashdeep_report.txt
    ├── extra_data
    │   ├── file.public
    │   ├── file.public.md5
    │   ├── to_ignored_dir -> ../ignored_dir
    │   └── to_ignored_file -> ../ignored_dir/ignored_file
    ├── pipeline
    │   ├── output
    │   │   ├── sample1
    │   │   │   └── results -> ../../work/sample1/results
    │   │   └── sample2 -> ../work/sample2
    │   └── work
    │       ├── sample1
    │       │   └── results -> /data/gpfs-1/work/users/blance_c/Development/saks/devel/tests/data/archive/project/pipeline/work/sample1/results
    │       └── sample2
    │           └── results -> /data/gpfs-1/work/users/blance_c/Development/saks/devel/tests/data/archive/project/pipeline/work/sample2/results
    ├── raw_data
    │   ├── batch1
    │   │   ├── sample1.fastq.gz
    │   │   └── sample1.fastq.gz.md5 -> /data/gpfs-1/work/users/blance_c/Development/saks/devel/tests/data/archive/outside/batch1/sample1.fastq.gz.md5
    │   ├── batch2
    │   │   ├── sample2.fastq.gz
    │   │   └── sample2.fastq.gz.md5 -> /data/gpfs-1/work/users/blance_c/Development/saks/devel/tests/data/archive/outside/batch2/sample2.fastq.gz.md5
    │   └── batch3
    │       ├── sample3.fastq.gz
    │       └── sample3.fastq.gz.md5 -> /data/gpfs-1/work/users/blance_c/Development/saks/devel/tests/data/archive/project/raw_data/batch3/sample3.fastq.gz.md5
    ├── README.md
    └── .snakemake.tar.gz


The inaccessible file ``project/extra_data/dangling_symlink`` & the contents of the ``project/ignored_dir`` are not present in the temporary destination, either because they are not accessible, or because they have been conscientiously ignored by the preparation step.

The ``.snakemake`` directory is replaced by the the gzipped tar file ``.snakemake.tar.gz`` in the temporary destination.

The ``file.public`` & the 3 ``*.fastq.gz`` files have been replaced by placeholder files of size 0. For ``file.public``, the md5 checksum has been computed by the preparing step, but for the ``*.fastq.gz`` files, the existing checksums are used.

All other files are kept for archiving: symlinks for real files point to their target's absolute path, symlinks are absolute for paths outside of the project, and relative for paths inside the project.

Finally, the hashdeep report of the original project directory is written to the temporary destination, and a ``README.md`` file is created. **At this point, we edit the ``README.md`` file to add a meaningful description of the project.** If a ``README.md`` file was already present in the orginial project directory, its content will be added to the newly created file.

Note that the symlinks ``temp_dest/extra_data/to_ignored_dir`` & ``temp_dest/extra_data/to_ignored_file`` are dangling, because the link themselves were not omitted, but their targets were. **This is the expected, but perhaps unwanted behaviour**: symlinks pointing to files or directories within compressed or ignored directories will be dangling in the temporary destination, as the original file exists, but is not part of the temporary destination.


Copy to the final destination
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When the ``README.md`` editing is complete, the copy to the final destination on the warm file system can be done. It is matter of ``cubi-tk archive copy temp_dest final_dest``.

The copy step writes in the final destination the hashdeep audit of the copy against the original project. This audit is expected to fail, because files & directories are ignored, compressed or squashed. The option ``--keep-workdir--hashdeep``, the programme also outputs the hashdeep report of the temporary destination, and the audit of the final copy against the temporary destination. Both the report and the audit are also stored in the final copy directory. The audit of the copy against the temporary destination should be successful, as the copy doesn't re-process files, it only follows symlinks.

If all steps have been completed successfully (including checking the ``README.md`` for validity), then a marker file named ``archive_copy_complete`` is created. The final step is to remove write permissions if the ``--read-only`` option was selected.
 

----------------------------
Additional notes and caveats
----------------------------

- Generally, the module doesn't like circular symlinks. It is wise to fix them before any operation, or use the rules facility to ignore them during preparation. The ``--dont-follow-links`` option in the summary step prevents against such problems, at the expense of missing some files in the report.
- The module is untested for symlink corner cases (for example, where a symlink points to a symlink outside of the project, which in turn points to another file in the project).
- In the archive, relative symlinks within the project are resolved. For example, in the original project one might have ``variants.vcf -> ../work/variants.vcf -> variants.somatic.vcf``. In the archive, the link will be ``variants.vcf -> ../work/variants.somatic.vcf``.

----------------
More Information
----------------

Also see ``cubi-tk archive --help``, ``cubi-tk archive summary --help``, ``cubi-tk archive prepare --help`` & ``cubi-tk archive copy --help`` for more information.
