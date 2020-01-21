.. _installation:

============
Installation
============

Prerequisites when using conda:

.. code-block:: bash

  $ conda create -n cubi-sak python=3.7
  $ conda activate cubi-sak

Clone CUBI-SAK and install:

.. code-block:: bash

  $ git clone git@cubi-gitlab.bihealth.org:CUBI/Pipelines/cubi-swiss-army-knife.git
  $ cd cubi-swiss-army-knife
  $ pip install -e .

For building the manual or running tests you will need some more packages.

.. code-block:: bash

  $ pip install -r requirements/develop.txt

Run tests:

.. code-block:: bash

  $ make test

Build manual

.. code-block:: bash

  $ cd docs_manual
  $ make clean html
