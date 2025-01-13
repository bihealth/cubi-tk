[![CI](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml/badge.svg)](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

# CUBI Toolkit

Tooling for connecting GitLab, pipelines, and SODAR at CUBI.

- [Documentation](https://cubi-tk.readthedocs.io/en/latest/?badge=latest)

## Getting Started

Prerequisites when using conda:

```bash
$ conda env create -n cubi-tk -f environment.yaml
$ conda activate cubi-tk
```

Clone CUBI-TK and install.

```bash
$ git clone git@github.com:bihealth/cubi-tk.git
$ cd cubi-tk
$ uv sync
$ uv pip install -e .
# or, if you need snappy kickoff:
#$ GIT_LFS_SKIP_SMUDGE=1 uv pip install -e '.[snappy]'
```

## Building the Manual

```bash
$ uv sync --all-extras --group docs
$ cd docs_manual
$ uv run make clean html
$ ls _build/html/index.html
```

## Argument Completion

```bash
$ cat >>~/.bashrc <<"EOF"
eval "$(register-python-argcomplete cubi-tk)"
EOF
```
