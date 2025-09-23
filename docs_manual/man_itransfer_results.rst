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

where each ``BLUEPRINT`` is the blueprint file mentioned above (probably "SODAR_export_blueprint.txt") and ``DESTINATION``
is either an iRODS path to a *landing zone* in SODAR, the UUID of that *landing zone* or a SODAR project UUID.

----------------
More Information
----------------

Also see ``cubi-tk sea-snap itransfer-results`` :ref:`CLI documentation <cli>` and ``cubi-tk sea-snap itransfer-results --help`` for more information.
