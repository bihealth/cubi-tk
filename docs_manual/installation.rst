.. _installation:

============
Installation
============

Clone CUBI-TK, create a conda environment and install using pip or `uv`_.

.. _uv: https://docs.astral.sh/uv/

Checkout the repository and create a conda environment:

.. code-block:: bash

    git clone git@github.com:bihealth/cubi-tk.git
    conda env create -n cubi-tk -f environment.yaml
    conda activate cubi-tk
    cd cubi-tk

Or to update an existing environment use:

.. code-block:: bash

    conda activate cubi-tk
    conda env update -f environment.yaml --prune


Install the package using pip:

.. code-block:: bash

    pip install -e .

Or using `uv`_:

.. code-block:: bash

    # if not using conda: `uv python install 3.12`
    uv python pin 3.12
    uv sync
    uv pip install -e .
    # alternatively, if you need snappy kickoff:
    # GIT_LFS_SKIP_SMUDGE=1 uv pip install -e '.[snappy]'



Run tests
---------

.. code-block:: bash

  uv run make pytest

Build manual
------------

.. code-block:: bash

  cd docs_manual
  uv run make clean html
