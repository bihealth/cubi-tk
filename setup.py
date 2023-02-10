#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path

from setuptools import find_packages, setup

import versioneer


def parse_requirements(path):
    """Parse ``requirements.txt`` at ``path``."""
    requirements = []
    with open(path, "rt") as reqs_f:
        for line in reqs_f:
            line = line.strip()
            if line.startswith("-r"):
                fname = line.split()[1]
                inner_path = os.path.join(os.path.dirname(path), fname)
                requirements += parse_requirements(inner_path)
            elif line != "" and not line.startswith("#"):
                requirements.append(line)
    return requirements


with open("README.md") as readme_file:
    readme = readme_file.read()

with open("CHANGELOG.md") as changelog_file:
    changelog = changelog_file.read()

test_requirements = parse_requirements("requirements/test.txt")
install_requirements = parse_requirements("requirements/base.txt")

setup(
    author="Manuel Holtgrewe, Patrick Pett",
    author_email=("manuel.holtgrewe@bih-charite.de"),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={"console_scripts": ("cubi-tk = cubi_tk.__main__:main",)},
    description="Tooling for connecting GitLab, pipelines, and SODAR at CUBI.",
    install_requires=install_requirements,
    license="MIT license",
    long_description=readme + "\n\n" + changelog,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="cubi_tk",
    name="cubi-tk",
    packages=find_packages(include=["cubi_tk"]),
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/bihealth/cubi-tk",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    zip_safe=False,
)
