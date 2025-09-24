.. _man_sodar_ingest_collection:

======================================
Manual for ``sodar ingest-collection``
======================================

The ``cubi-tk sodar ingest`` command can be used to upload an arbitrary set of data files to SODAR.
It facilitates transfer of one or multiple sources into a single SODAR landing zone collection,
while optionally recursively matching and preserving the sub-folder structure.

-----------
Basic usage
-----------

.. code-block:: bash

    $ cubi-tk sodar ingest [OPTION]... SOURCE [SOURCE ...] DESTINATION

Where each ``SOURCE`` is a path to a folder containing files and ``DESTINATION`` is either a SODAR iRODS path,
a *landing zone* UUID, or a SODAR project UUID.

For seamless usage `~/.irods/irods_environment.json <https://sodar-server.readthedocs.io/en/dev/ui_irods_info.html>`_
and :ref:`~/.cubitkrc.toml <sodar_setup>` should be present.

----------
Parameters
----------

- ``-r, --recursive``: Recursively find files in subdirectories and create iRODS sub-collections to match directory structure.
- ``-e, --exclude``: Exclude files matching the given pattern.
- ``-s, --sync``: Skip upload of files that already exist in iRODS.
- ``-K, --remote-checksums``: Instruct iRODS to compute MD5 checksums of uploaded files for SODAR validation step.
- ``-y, --yes``: Don't stop for user permission. Enables scripting with this command.
- ``--collection``: Set target iRODS collection in landing zone. Skips user input for this selection.


----------------
More Information
----------------

Also see ``cubi-tk sodar ingest-collection`` :ref:`CLI documentation <cli>` and ``cubi-tk sodar ingest-collection --help`` for more information.
