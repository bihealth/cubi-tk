.. _man_write_sample_info:

=========================================
Manual for ``sea-snap write-sample-info``
=========================================

The ``cubi-tk sea-snap write-sample-info`` command can be used to collect information by parsing the folder structure of raw data files (FASTQ) and meta-information (ISA-tab).
It collects this information in a YAML file that will be loaded by the Seasnap pipeline.

The basic usage is:

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info IN_PATH_PATTERN

where ``IN_PATH_PATTERN`` is a file path with wildcards specifying the location to FASTQ files.
The wildcards are also used to extract information from the parsed paths.

By default, a file called ``sample_info.yaml`` will be generated in the current working directory.
If this file is in the project working directory, Seasnap will load it automatically.
However, you can specify another file name after ``IN_PATH_PATTERN``.
Then this file can be used in Seasnap e.g. like so:

.. code-block:: bash

    $ ./sea-snap mapping l --config file_name='sample_info_alt.yaml'

**Note: check and edit the auto-generated sample_info.yaml file before running the pipeline.**

--------------------------
Path pattern and wildcards
--------------------------

For example, if the FASTQ files are stored in a folder structure like this:

::

    input
    ├── sample1
    │   ├── sample1_R1.fastq.gz
    │   └── sample1_R2.fastq.gz
    └── sample2
        ├── sample2_R1.fq
        └── sample2_R2.fq

Then the path pattern can look like the following:

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info "input/{sample}/*_{mate,R1|R2}"

Keywords in braces (e.g. ``{sample}``) are wildcards.
It is possible to add a regular expression separated with a comma after the keyword.
This is useful to restrict what part of the file path the wildcard can match (e.g. ``{mate,R1|R2}`` means that mate can only be ``R1`` or ``R2``).
In addition, ``*`` and ``**`` can be used to match anything that does not need to be captured with a wildcard.

Setting the ``IN_PATH_PATTERN`` as shown above will allow the ``write-sample-info`` command to extract the information that samples *sample1* and *sample2* exist and that there are *paired reads* for both of them.
The extension (e.g. ``fastq.gz``, ``fastq`` or ``fq``) should be omitted and will be detected automatically.

Available wildcards are: ``{sample}``, ``{mate}``, ``{flowcell}``, ``{lane}``, ``{batch}`` and ``{library}``.
However, only **``{sample}``** is obligatory.

**Note: wildcards do not match ``/`` and``.``.**
For further information also see the `Seasnap docu <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/sea-snap/-/blob/development/documentation/prepare_input.md>`_.

----------------
Meta information
----------------

When working with **SODAR**, additional meta-information should be included in the sample info file.
In SODAR this meta-information is stored in the form of `ISA-tab files <https://isa-specs.readthedocs.io/en/latest/isatab.html#>`_.

There are two ways to add the information from an ISA-tab assay file to the generated sample info file:

1. Load from a local ISA-tab assay file

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info --isa-assay PATH/TO/a_FILE_NAME.txt IN_PATH_PATTERN

2. Download from SODAR

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info --project_uuid UUID IN_PATH_PATTERN

Here, ``UUID`` is the UUID of the respective project on SODAR.

--------------------
SODAR authentication
--------------------

To be able to access the SODAR API (which is only required if you download meta-data from SODAR), you also need an API token.
For token management for SODAR, the following docs can be used:

- https://sodar.bihealth.org/manual/ui_user_menu.html
- https://sodar.bihealth.org/manual/ui_api_tokens.html

There are three options how to supply the token.
Only one is needed.
The options are the following:

1. configure ``~/.cubitkrc.toml``.

    .. code-block:: toml

        [global]

        sodar_server_url = "https://sodar.bihealth.org/"
        sodar_api_token = "<your API token here>"

2. pass via command line.

    .. code-block:: bash

        $ cubi-tk sodar ingest-fastq --sodar-url "https://sodar.bihealth.org/" --sodar-api-token "<your API token here>"

3. set as environment variable.

    .. code-block:: bash

        $ SODAR_API_TOKEN="<your API token here>"

------------
Table format
------------

Although this is not really necessary to run the workflow, it is possible to convert the YAML file to a table / sample sheet:

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info --from-file sample_info.yaml XXX sample_info.tsv

And back:

.. code-block:: bash

    $ cubi-tk sea-snap write-sample-info --from-file sample_info.tsv XXX sample_info.yaml

----------------
More Information
----------------

Also see ``cubi-tk sea-snap write-sample-info`` :ref:`CLI documentation <cli>` and ``cubi-tk sea-snap write-sample-info --help`` for more information.