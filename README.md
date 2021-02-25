![Continuous Integration Status](https://github.com/bihealth/cubi-tk/workflows/CI/badge.svg)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/71dd0ea53e444cd0949a00a7025face7)](https://www.codacy.com/gh/bihealth/cubi-tk/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=bihealth/cubi-tk&amp;utm_campaign=Badge_Grade)
[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/71dd0ea53e444cd0949a00a7025face7)](https://www.codacy.com/gh/bihealth/cubi-tk/dashboard?utm_source=github.com&utm_medium=referral&utm_content=bihealth/cubi-tk&utm_campaign=Badge_Coverage)
[![Documentation Status](https://readthedocs.org/projects/cubi-tk/badge/?version=latest)](https://cubi-tk.readthedocs.io/en/latest/?badge=latest)
[![MIT License](https://img.shields.io/badge/License-MIT-green.svg)](https://opensource.org/licenses/MIT)

# CUBI Toolkit

Tooling for connecting GitLab, pipelines, and SODAR at CUBI.

- [Documentation](https://cubi-tk.readthedocs.io/en/latest/?badge=latest)

## Getting Started

Prerequisites when using conda:

```bash
$ conda create -n cubi-tk python=3.7
$ conda activate cubi-tk
```

Clone CUBI-SAK and install.

```bash
$ git clone git@cubi-gitlab.bihealth.org:CUBI/Pipelines/cubi-tk.git
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
