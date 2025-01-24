[![CI](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml/badge.svg)](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

# CUBI Toolkit

Tooling for connecting GitLab, pipelines, and SODAR at CUBI.

- [Documentation](https://cubi-tk.readthedocs.io/en/latest/?badge=latest)

## Getting Started

Clone CUBI-TK, create a conda environment and install using pip or [`uv`](https://docs.astral.sh/uv/).

Checkout the repository and create a conda environment:
```bash
git clone git@github.com:bihealth/cubi-tk.git
conda env create -n cubi-tk -f environment.yaml
conda activate cubi-tk
cd cubi-tk
```

Install the package using pip:
```bash
$ pip install -e .
```

Or using `uv`:
```bash
# if not using conda: `uv python install 3.12`
uv python pin 3.12
uv sync
uv pip install -e .
# alternatively, if you need snappy kickoff:
# GIT_LFS_SKIP_SMUDGE=1 uv pip install -e '.[snappy]'
```

## Building the Manual

```bash
uv sync --all-extras --group docs
cd docs_manual
uv run make clean html
xdg-open _build/html/index.html
```

## Argument Completion

```bash
cat >>~/.bashrc <<"EOF"
eval "$(register-python-argcomplete cubi-tk)"
EOF
```
