.. _installation:

============
Installation
============

Prerequisites when using conda:

.. code-block:: bash

  $ conda env create -n cubi-tk -f environment.yaml
  $ conda activate cubi-tk

Clone CUBI-TK and install:

.. code-block:: bash

  $ git clone git@github.com:bihealth/cubi-tk.git
  $ cd cubi-tk
  $ uv python pin 3.12
  $ uv sync
  $ uv pip install -e .
  # or, if you need snappy kickoff:
  #$ GIT_LFS_SKIP_SMUDGE=1 uv pip install -e '.[snappy]'


Run tests
---------

.. code-block:: bash

  $ uv run make pytest

Build manual
------------

.. code-block:: bash

  $ cd docs_manual
  $ uv run make clean html
