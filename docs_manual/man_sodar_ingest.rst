.. _man_sodar_ingest:

===========================
Manual for ``sodar ingest``
===========================

The ``cubi-tk sodar ingest`` command can be used to upload arbitrary data files to SODAR.
It facilitates transfer of one or multiple sources into one SODAR landing zone, while optionally recursively matching and preserving the sub-folder structure.

----------------
Basic usage
----------------

.. code-block:: bash

    $ cubi-tk sodar ingest [OPTION]... SOURCE [SOURCE ...] DESTINATION

Where each ``SOURCE`` is a path to a folder containing files and ``DESTINATION`` is either a SODAR iRODS path or a *landing zone* UUID.

For seamless usage `~/.irods/irods_environment.json <https://sodar-server.readthedocs.io/en/dev/ui_irods_info.html>`_ and :ref:`~/.cubitkrc.toml<sodar-auth>` should be present.
This command automatically handles your iRODS session and authentication (i.e. `iinit`).

----------------
Parameters
----------------

- ``-r, --recursive``: Recursively find files in subdirectories and create iRODS sub-collections to match directory structure.
- ``-e, --exclude``: Exclude files matching the given pattern.
- ``-s, --sync``: Skip upload of files that already exist in iRODS.
- ``-K, --remote-checksums``: Instruct iRODS to compute MD5 checksums of uploaded files for SODAR validation step.
- ``-y, --yes``: Don't stop for user permission. Enables scripting with this command.
- ``--collection``: Set target iRODS collection in landing zone. Skips user input for this selection.

.. _sodar-auth:

--------------------
SODAR authentication
--------------------

To be able to access the SODAR API (which is only required, if you specify a landing zone UUID instead of an iRODS path), you also need an API token.
For token management in SODAR, the following docs can be used:

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

Also see ``cubi-tk sodar ingest`` :ref:`CLI documentation <cli>` and ``cubi-tk sodar ingest --help`` for more information.
