.. _man_ingest_fastq:

=================================
Manual for ``sodar ingest-fastq``
=================================

The ``cubi-tk sodar ingest-fastq`` command lets you upload raw data files to SODAR.
It is configured for uploading FASTQ files by default, but the parameters can be adjusted to upload any files.

The basic usage is:

.. code-block:: bash

    $ cubi-tk sodar ingest-fastq SOURCE [SOURCE ...] DESTINATION

where each ``SOURCE`` is a path to a folder containing relevant files (this can also be a WebDav URL, see below) and
``DESTINATION`` is either an iRODS path to a *landing zone* in SODAR, the UUID of that *landing zone*, or and SODAR project UUID.

----------------
Other file types
----------------

By default, the parameters ``--src-regex`` and ``--remote-dir-pattern`` are configured for FASTQ files, but they may be changed to upload other files as well.
The two parameters have the following functions:

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

It contains the wildcard ``{collection_name}``, which represents and irods collection and will be filled with the captured
content of group ``(?P<sample>...)``, potentially modified by a regex (see 'Mapping of file names' below).
Alternatively the irods collection name can be derived by mapping the extracted (and modified) ``(?P<sample>...)``
group to any column of the assay table associated with the LZ or project. In this case the ``{library_name}`` will be
filled with the content of the last material column of the assay table (or ``--collection-column`` if defined).
In addition, the wildcards ``{date}`` and ``{filename}`` can always be used in ``--remote-dir-pattern`` and will be
filled with the current date (or ``--remote-dir-date``) and full filename (the basename of a matched file), respectively.

---------------------
Mapping of file names
---------------------

In some cases additional mapping of filenames is required (for example to fully macth the irods collections).
This can be done via the parameter ``--sample-collection-mapping`` or short ``-m``.
It can be supplied several times, each time for another mapping.
With each ``-m MATCH REPL`` a pair of a regular expression and a replacement string are specified.
Internally, pythons ``re.sub`` command is executed on the extracted ``(?P<sample>...)`` capture group.
Therefore, you can refer to the documentation of the `re package <https://docs.python.org/3/library/re.html>`_ for syntax questions.

----------------------
Source files on WevDAV
----------------------

If a ``SOURCE`` is a WebDAV url, the files will temporarily be downloaded into a directory called "./temp/".
This can be adjusted with the ``--tmp`` option.

--------------------
SODAR authentication
--------------------

To use this command, which internally executes iRODS icommands, you need to authenticate with iRODS by running:

.. code-block:: bash

    $ iinit

To be able to access the SODAR API you also need an API token. For token management for SODAR, the following docs can be used:

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

----------------
More Information
----------------

Also see ``cubi-tk sodar ingest-fastq`` :ref:`CLI documentation <cli>` and ``cubi-tk sodar ingest-fastq --help`` for more information.
