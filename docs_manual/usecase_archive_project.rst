.. _usecase_archive:

=============================
Use Case: Archiving a project
=============================

This section describes the process of archiving a project using ``cubi-tk``.
This section provides an example of how cubi-tk can be used in different cases.

--------
Overview
--------

The general process to archive projects is:

1. Get acquainted with the contents of the projects directory.
   The command ``cubi-tk archive summary`` provides a basic facility to identify several important aspects for the archival process.
   It does not, however, check whether files are already stored on SODAR. This must be done independently.
2. Archives **must** be accompanied by a ``README.md`` file, which provides important contact information about the project's scientific P.I.,
   e-mail addresses of the post-doc in charge, the individuals in CUBI that processed the data, and the person in charge of the archive.
   URLs for SODAR & Gitlab are also important.
   The command ``cubi-tk archive readme`` creates a valid README file, that contains these informations.
3. In many cases, not all files should be archived: there is no need to duplicate large sequencing files (fastrq or bam) if they are already safely stored on SODAR.
   Likewise, whole genome sequence, annotations, indices, should not be archived in most cases.
   The command ``cubi-tk archive prepare`` identifies files that must be copied, and those which shouldn't.
   (it can do a bit more, see below).
4. Once these preparation steps have been carried out, the command ``cubi-tk archive copy`` performs the copy of the project to its final archive destination.
   This command creates checksums for all files in the project, and in the archive copy. It provides an audit of the comparison between these two sets of checksums,
   to ensure trhat the archival was successful.

Each of these steps descibed above are discussed below, to give practical examples, and to suggest good practice.

-------
Summary
-------

The summarisation step aims to report several cases of files that may require attention for archiving.
In particular, symbolic links to destinations outside of the project's directory should be reported.
Dangling symbolic links (either because the target is missing, or because of permissions) are also listed.

The module also lists specific files of interest. By default, large bam or fastq files (larger than 256MB)
are reported, as well as large fasta files, annotations (with ``.gtf`` or ``.gff`` extensions), and
short-read-archive sequencing data.

It is possible for the user to change the reporting criteria, using a ``yaml`` file & the ``--classes`` option.
For example:

    .. code-block:: bash

        $ cubi-tk archive summary \
            --classes reporting_classes.yaml \  # Use your own reporting selection
            <project_directory> \
            <summary file>

The default summary classes can be found in ``<cubi-tk installation>/cubi_tk/archive/classes.yaml``.
Its content reads:

    .. code-block:: yaml

        fastq:
            min_size: 268435456
            pattern: "^(.*/)?[^/]+(\\.f(ast)?q(\\.gz)?)$"
        bam:
            min_size: 268435456
            pattern: "^(.*/)?[^/]+(\\.bam(\\.bai)?)$"
        public:
            min_size: 268435456
            pattern: "^(.*/)?(SRR[0-9]+[^/]*|[^/]+\\.(fa(sta)?|gtf|gff[23]?)(\\.gz)?)$"

The output of the summarization is a table, with the reason why the file is reported in the first column,
the file name, the symlink target if the file is a symlink, the file's normalised path, its size,
and, in case of symlinks, if the target is accessible, and if it is inside the project or not.


--------------------
Readme file creation
--------------------

The module creates README files that **must** contain contact information to

- The project's scientific P.I. (Name & email address),
- The contact to the person in charge of the project, very often a post-doc in the P.I.'s group (name & e-mail address),
- The contact to the person who is archiving the project (name & e-mail address). This person will be the project's contact in CUBI.
- The name of the person who actually did the data processing & analysis in CUBI.
  It is generally the same person who is archiving the project, unless he or she has left CUBI.

The SODAR & Gitlab's URLs should also be present in  the README file, when applicable.
But this information is not mandatory, unlike the contact information.

**Important notes**

The creation of the README file is a frequent source of errors and frustrations.
To minimize the inconveniences, please heed these wise words.

- E-mail addresses must be present, valid & cannot contain uppercase letters (don't ask why...)
- Generally, the module is quite fussy about the format. Spaces, justification, ... may be important.
- Upon README creation, the project directory is quickly scanned to generate an overview of the
  project's size and number of inodes. For large projects, it is possible to disable this behaviour
  using the ``--skip-collect`` option.
- Because of these problems, the module offers a possibility to check README file validity. The command is
  `cubi-tk archive readme --is-valid project_dir readme_file`.
- If a README file is already present in the project, it will be appended at the bottom of the
  README file generated by the module.

Most importantly, please edit your README file after generation by the module. The module generates
no description of the aims & results of the project, even though it is very useful and important to have.


-----------------------
Preparation of the copy
-----------------------

During preparation, the user can select the files that will be archived, those that will be discarded,
and those that must be processed differently.

The file selection is achieved by creating a temporary copy of the project's directory structure,
using symbolic links. The location of this temporary copy is called *temporary destination*.

When copying a file to this temporary destination, its fate is decided based on its filename & path,
using regular expression pattern matching. There are 4 types of operations:

- The files are selected for copy. This is the default behaviour.
- Files can be omitted (or *ignored*) from the copy.
- Directories with many (smallish) files can be tarred & compressed to reduce the total number of inodes (which is very file-system friendly).
- Finally, files can be *squashed*. In this case, a file will have its md5 checksum computed and seved in a companion files next to it, and
  the file will finally be replaced with a placeholder with the same name, but with a size  of 0.
  This is useful for large files that can easily be downloaded again from the internet.
  Public sequencing datasets, genome sequences & annotations are typical examples.

The user can impose its own rules, based on the content of the project.
The selection rules are defined in a yaml file accessed through the module's ``--rules`` option.
The default rules file is in ``<cubi-tk installation>/cubi_tk/archive/default_rules.yaml``,
and its content reads:

    .. code-block:: yaml

        ignore:            # Patterns for files or directories to skip
            - "^(.*/)?core\\.[0-9]+$"   # Ignore core dumps
            - "^(.*/)?\\.venv$"         # Ignore virtual environment .venv directories

        compress:          # Patterns for files or directories to tar-gzip
            - "^(.*/)?\\.snakemake$"    # Created by snakemake process
            - "^(.*/)?sge_log$"         # Snappy SGE log directories
            - "^(.*/)?\\.git$"          # Git internals
            - "^(.*/)?snappy-pipeline$" # Copy of snappy
            - "^(.*/)?cubi_wrappers$"   # Copy of snappy's ancestor

        squash: []         # Patterns for files to squash (compute MD5 checksum, and replace by zero-length placeholder)


**Important notes**

- The temporary destination is typically chosen as ``/fast/scratch/users/<user>/Archive/<project_name>``.
- The README file generated in the previous step is copied to the temporary destination using the module's ``--readme`` option.
- When the temporary destination is complete, the module creates a complete list of all files accessible from the original project directory,
  and computes md5 & sh256 checksums, using ``hashdeep``.
  This is done **for all files accessible from the project's directory**, including all symbolic links.
- The computation of checksums can be extremely time-consuming. Multiple threads can be used with the ``--num-threads`` option.
  Nevertheless, in most cases, it is advisable to submit the preparation as a slurm job, rather than interactively.


Example of usage:

.. code-block:: bash

    $ cubi-tk archive prepare \
        --rules <my_rules>    \        # Project-specific rules
        --readme <my_readme>  \        # README.md file generated in the previous step
        --ignore-tar-errors   \        # Useful only in cases of inaccessible files to compress
        <project_dir>         \
        <termporary_destination>


-------------------------
Copy to final destination
-------------------------

The last step consist in copying all files in the temporary destination to the archiving location.
This is done internally using ``rsync``, having previously removed all symbolic links connecting files wihtin the project directory.
These *local* symbolic links are restored after the copy is complete, in both the temporary & final destinations.
After the copy is complete, the archiving directory can be protected against writing with the ``--read-only`` option.

A verification based on md5 checksums is automatically done between the original project directory and the final copy.
In most cases, differences between the directories are expected, because of the files ignored, compressed and squashed.
However, it is good practice to examine the audit file to make sure that all files missing from the copy are missing for the right reasons.
The report of checksums of all files in the original project, and the audit result are both present in the final destination,
as files called ``<date>_hashdeep_report.txt`` and ``<date>_hashdeep_audit.txt`` respectively.

For additional verification, it is also possible to request (using the ``--keep-workdir-hashdeep`` option) a hashdeep report of the
temporary destination, and the corresponding audit of the final copy. These contents of these two directories
are expected to be identical, and any discrepancy should be looked at carefully.
The report & audit files relative to the temporary destination are called ``<date>_workdir_report.txt`` & ``<date>_workdir_audit.txt``.

Finally, the copy and hasdeep steps are quite time-consuming, and it is good practice to submit the copy as a slurm job
rather than interactively, even when multiple threads are used (through the ``--num-threads`` option).

An example of a copy script that can be submitted to slurm is:

.. code-block:: bash

    #!/bin/bash

    #SBATCH --job-name=copy
    #SBATCH --output=slurm_log/copy.%j.out
    #SBATCH --error=slurm_log/copy.%j.err
    #SBATCH --partition=medium
    #SBATCH --mem=4000
    #SBATCH --time=72:00:00
    #SBATCH --ntasks=1
    #SBATCH --cpus-per-task=8

    # ------------------ Command-line options -----------------------------

    # Taken from https://stackoverflow.com/questions/402377/using-getopts-to-process-long-and-short-command-line-options
    TEMP=$(getopt -o ts:d: --long dryrun,source:,destination: -- "$@")

    if [ $? != 0 ] ; then echo "Terminating..." >&2 ; exit 1 ; fi

    # Note the quotes around '$TEMP': they are essential!
    eval set -- "$TEMP"

    dryrun=0
    src=""
    dest=""
    while true; do
        case "$1" in
            -t | --dryrun ) dryrun=1; shift ;;
            -s | --source ) src="$2"; shift 2 ;;
            -d | --destination ) dest="$2"; shift 2 ;;
            -- ) shift; break ;;
            * ) break ;;
        esac
    done

    if [[ "X$src" == "X" ]] ; then echo "No project directory defined" >&2 ; exit 1 ; fi
    if [[ ! -d "$src" ]] ; then echo "Can't find project directory $src" >&2 ; exit 1 ; fi
    if [[ "X$dest" == "X" ]] ; then echo "No temporary directory defined" >&2 ; exit 1 ; fi
    if [[ -e "$dest" ]] ; then echo "Temporary directory $dest already exists" >&2 ; exit 1 ; fi

    if [[ dryrun -eq 1 ]] ; then
        echo "cubi-tk archive copy "
        echo "--read-only --keep-workdir-hashdeep --num-threads 8 "
        echo "\"$src\" \"$dest\""
        exit 0
    fi

    # ---------------------- Subtmit to slurm -----------------------------

    export LC_ALL=en_US
    unset DRMAA_LIBRARY_PATH

    test -z "${SLURM_JOB_ID}" && SLURM_JOB_ID=$(date +%Y-%m-%d_%H-%M)
    mkdir -p slurm_log/${SLURM_JOB_ID}

    CONDA_PATH=$HOME/work/miniconda3
    set +euo pipefail
    conda deactivate &>/dev/null || true  # disable any existing
    source $CONDA_PATH/etc/profile.d/conda.sh
    conda activate cubi_tk # enable found
    set -euo pipefail

    cubi-tk archive copy \
        --read-only --keep-workdir-hashdeep --num-threads 8 \
        "$src" "$dest"

