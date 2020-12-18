# CUBI Toolkit

Tooling for connecting GitLab, pipelines, and SODAR at CUBI.

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
