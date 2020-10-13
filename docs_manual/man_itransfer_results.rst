.. _man_seasnap_itransfer_results:

=========================================
Manual for ``sea-snap itransfer-results``
=========================================

The ``cubi-tk sea-snap itransfer-results`` command lets you upload results of the Seasnap pipeline to SODAR.
It relies on running the ``export`` function of Seasnap first.
This ``export`` function allows to select which result files of the pipeline shall be uploaded into what folder structure, which can be configured via the Seasnap config file.
It outputs a ``blueprint`` file with file paths and commands to use for the upload.
For more information see the `Seasnap documentation <https://cubi-gitlab.bihealth.org/CUBI/Pipelines/sea-snap/-/blob/development/documentation/export.md>`_
The ``itransfer-results`` function parallelizes the upload of these files.

The basic usage is:

1. create blueprint

.. code-block:: bash

    $ ./sea-snap mapping l export

2. upload to SODAR

.. code-block:: bash

    $ cubi-tk sea-snap itransfer-results BLUEPRINT DESTINATION

where each ``BLUEPRINT`` is the blueprint file mentioned above (probably "SODAR_export_blueprint.txt") and ``DESTINATION`` is either an iRODS path to a *landing zone* in SODAR or the UUID of that *landing zone*.

--------------------
SODAR authentication
--------------------

To use this command, which internally executes iRODS icommands, you need to authenticate with iRODS by running:

.. code-block:: bash

    $ iinit

To be able to access the SODAR API (which is only required, if you specify a landing zone UUID instead of an iRODS path), you also need an API token.
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

----------------
More Information
----------------

Also see ``cubi-tk sea-snap itransfer-results`` :ref:`CLI documentation <cli>` and ``cubi-tk sea-snap itransfer-results --help`` for more information.