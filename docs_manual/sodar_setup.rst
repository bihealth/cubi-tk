.. _sodar_setup:

=============
cubi-tk setup
=============

Setup for Sodar API access
--------------------------

``cubi-tk`` will in many cases access `SODAR <https://sodar-server.readthedocs.io/en/latest/>`_ (through its web API)
and the connected iRODS file system.
To be able to access the SODAR API you will need an API token. For token creation and management in SODAR, please consult the
`respective documentation <https://sodar-server.readthedocs.io/en/latest/ui_api_tokens.html>`_


There are two options how to supply the API token and the sodar server url, but only one is needed.
The options are the following:

1. configure ``~/.cubitkrc.toml``.

    .. code-block:: toml

        [global]

        sodar_server_url = "https://sodar.bihealth.org/"
        sodar_api_token = "<your API token here>"

2. pass via command line.

    .. code-block:: bash

        $ cubi-tk sodar ingest-data --sodar-url "https://sodar.bihealth.org/" --sodar-api-token "<your API token here>"


Setup for irods access
----------------------

In order for ``cubi-tk`` to upload and download files to or from SODAR it also needs to authenticate with the irods server
used by SODAR. This requires a specific file `~/.irods/irods_environment.json` which can be
`obtained form Sodar <https://sodar-server.readthedocs.io/en/latest/ui_irods_info.html>`_.

Once this file is present, ``cubi-tk`` will then attempt a login on the irods server, which still requires you to enter your password.
The login token is cached, so that a re-authentication is only rarely needed.


Setup for multiple SODAR instances
----------------------------------

``cubi-tk`` supports easy switching between different SODAR instances and their respective irods servers. To use this
feature you need to use the ``~/.cubitkrc.toml`` file, where you can enter additional sodar server profiles:


.. code-block:: toml

    [global]
    sodar_server_url = "https://sodar.bihealth.org/"
    sodar_api_token = "<your API token here>"

    [staging]
    sodar_server_url = "https://sodar-staging.bihealth.org/"
    sodar_api_token = "<your 2nd API token here>"


In addition to the additional server URL and API token, you will also need an additional "irods_environment" file.
Download this file from the new SODAR server as usual, but then change the name you place it under in your home directory:
`~/.irods/irods_environment_staging.json`, the file name addition needs to match the entry in the toml file for cubi-tk
to properly allocate it.

When you have multiple servers defined like this, you can easily switch using the ``--config-profile`` option, which takes
the name of a profile (i.e. `staging`). The `global` profile is always used a default, if nothing else is specified.

Changing the sodar profile will generally require re-authentication in irods.
