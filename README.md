[![CI](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml/badge.svg)](https://github.com/bihealth/cubi-tk/actions/workflows/main.yml)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

# CUBI Toolkit

Tooling for connecting GitLab, pipelines, and SODAR at CUBI.

- [Documentation](https://cubi-tk.readthedocs.io/en/latest/?badge=latest)

## Getting Started

Prerequisites when using conda:

```bash
$ conda create -n cubi-tk python=3.10
$ conda activate cubi-tk
```

First install snappy-pipeline, which is required for some snappy commands:

```bash
$ git clone https://github.com/bihealth/snappy-pipeline
$ cd snappy-pipeline
$ pip install -e .
```

Clone CUBI-TK and install.

```bash
$ git clone git@github.com:bihealth/cubi-tk.git
$ cd cubi-tk
$ pip install -e .
```

## Building the Manual

```bash
$ pip install -r requirements/develop.txt
$ cd docs_manual
$ make clean html
$ ls _build/html/index.html
```

## Argument Completion

```bash
$ cat >>~/.bashrc <<"EOF"
eval "$(register-python-argcomplete cubi-tk)"
EOF
```
