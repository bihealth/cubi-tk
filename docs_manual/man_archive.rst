.. _man_archive:

======================
Manual for ``archive``
======================

The ``cubi-tk archive`` is designed to facilitate the archival of older projects away from the cluster's fast filesystem.
This document provides an overview of these commands, and how they can be adapted to meet specific needs.

---------------------------------
Background: the archiving process
---------------------------------

CUBI archive resources are three-fold:

- SODAR & storage should contain raw data generated for the project. SODAR also contains important results (mapping, variants, differential expression, ...).
- Gitlab containts small files required to generate the results, typically scripts, configuration files, READMEs, meeting notes, ..., but also knock-in gene sequence, list of papers, gene lists, ...
- The rest should be stored in the CEPH filesystem, for example intermediate results produced by older pipelines.

For older project, the effort of uploading the data to SODAR & gitlab may not be warranted. In this case, the bulk of the archive might be stored in the CEPH filesystem.

**The module aims to facilitate this last step, i.e. the archival of old projects to the CEPH system.**

------------------------------
Archiving process requirements
------------------------------

Archived projects should contain all _important_ files, but not data already stored elsewhere. In particular, the following files should be be archived:

- raw data (``*.fastq.gz`` files) saved in SODAR or in the ``STORE``,
- data from public repositories (SRA, GDC portal, ...) that can easily be downloaded again,
- static data such as genome sequence & annotations, variant databases from gnomaAD, ... that can also be easily retrieved,
- indices files for mapping that can be re-generated.

**Importantly, a README file should be present in the archive, briefly describing the project, listing contacts to the client & within CUBI and providing links to SODAR & Gitlab when appropriate.**


**The purpose of the module is:**

- to provide a summary of files that should not be archived, or that are problematic for any reason,
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

    $ cubi-tk archive prepare PROJECT_DIRECTORY DESTINATION

``DESTINATION`` is here the path to the temporary directory that will be created. It must not exist.

For each file that must be archived, the module creates a symlink to that file's absolute path. The module also reproduces the project's directories hierarchy, so that the symlink sits in the same relative position in the temporary directory than in the original project.

The module deals with symlinks in the project differently whether their target in inside the project or not. For symlinks pointing outside of the project, a symlink to the target's absolute path is created. For symlinks pointing inside the project, a relative path symlink is created. This allows to store all files (even those outside of the project), without duplicating symlinks inside the project.

Finally, the contents of the ``.snakemake`` directories are processed differently: the directories are tarred & compressed in the temporary destination, to reduce the number of inodes in the archive. 

**Copy to archive & verifications**

Not yet implemented


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

Three different archving options are implemented:

- **ignore**: the files or directories matching the pattern are simply omitted from the temporary destination. This is useful to ignore remaining temporary files, core dumps or directories containing lists of input symlinks, for example.
- **compress**: the files or directories matching the pattern will be replaced in the temporary destination by a compressed (gzipped) tar file. This is how ``.snakemake`` files are treated by default, but patterns for other directories may be added, for example for the SGE or Slurm log directories.
- **squash**: the files matching the pattern will be replaced by zero-length placeholders in the temporary destination. A md5 checksum file will be added next to the original file, to enable verification.


--------
Examples
--------

Consider an example project with the following architecture::

    project_dir
    ├── .snakemake
    │   └── snakemake
    ├── file.public
    ├── files
    │   ├── archived
    │   └── ignored.pattern
    ├── ignored_dir
    │   └── dummy_file
    └── symlinks
        ├── accessible -> ../../outside/accessible
        ├── dangling -> ../files/missing_file
        ├── to_archived -> ../files/archived
        ├── to_ignored_dir -> ../ignored_dir
        ├── to_ignored.pattern -> ../files/ignored.pattern
        └── to_inaccessible -> ../../outside/protected/inaccessible_file


After running the preparation command ``cubi-tk archive prepare project_dir temp_dest_dir``, the temporary destination contains the following files::

    temp_dest_dir
    ├── .snakemake.tar.gz
    ├── file.public -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/project/file.public
    ├── files
    │   ├── archived -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/project/files/archived
    │   └── ignored.pattern -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/project/files/ignored.pattern
    ├── ignored_dir
    │   └── dummy_file -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/project/ignored_dir/dummy_file
    └── symlinks
        ├── accessible -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/outside/accessible
        ├── to_archived -> ../files/archived
        ├── to_ignored_dir -> ../ignored_dir
        └── to_ignored.pattern -> ../files/ignored.pattern


The inaccessible files ``project/symlinks/dangling`` & ``project/symlinks/to_inaccessible`` are not present in the temporary destination. All other files are kept for archiving: symlinks for real files point to their target's absolute path, symlinks are absolute for paths outside of the project, and relative for paths inside the project, and the ``.snakemake`` directory has been tarred & compressed. 

Now if we want to ignore the ``project/ignored_dir`` directory and the files with extension ``*.pattern``, and to squash the public file with extension ``*.public``, we use the following yaml rule file:

.. code-block:: yaml

    ignore:
        - "^(.*/)?ignored_dir$"
        - "^(.*/)?.+\\.pattern$"
    
    squash:
        - "^(.*/)?.+\\.public$"
    
    compress:
        - "^(.*/)?.snakemake$"


The output directory for the ``cubi-tk archive prepare --rules rules_with_ignore.yaml project_dir temp_dest_dir`` command becomes::

    temp_dest_dir
    ├── .snakemake.tar.gz
    ├── file.public
    ├── file.public.md5
    ├── files
    │   └── archived -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/project/files/archived
    └── symlinks
        ├── accessible -> /fast/work/users/blance_c/Development/saks/archive/cubi_tk/archive/outside/accessible
        ├── to_archived -> ../files/archived
        └── to_ignored_dir -> ../ignored_dir


The ``project/ignored_dir`` directory and the files with extension ``*.pattern`` are not in the temporary destination, the ``temp_dest_dir/file.public`` is an empty file with the md5 checksum of ``project/file.public`` in ``temp_dest_dir/file.public.md5``. However, the symlink ``temp_dest_dir/symlinks/to_ignored_dir`` is dangling, because the link itself was not omitted, but its destination was. **This is the expected, but perhaps unwanted behaviour**: symlinks pointing to files or directories within compressed or ignored directories will be dangling in the temporary destination, as the original file exists, but is not part of the temporary destination.


----------------
More Information
----------------

Also see ``cubi-tk archive --help``, ``cubi-tk archive summary --help``, ``cubi-tk archive prepare --help`` & ``cubi-tk archive copy --help`` for more information.
