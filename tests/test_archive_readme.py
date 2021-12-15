"""Tests for ``cubi_tk.archive.prepare``.

We only run some smoke tests here.
"""

import collections
import os
import tempfile

import cubi_tk.archive.readme


def test_run_archive_readme_smoke_test():
    with tempfile.TemporaryDirectory() as tmp_dir:
        project_name = "project"
        project_dir = os.path.join(os.path.dirname(__file__), "data", "archive", project_name)

        config = {
            "sodar_server_url": "https://sodar.bihealth.org",
            "var_PI_name": "Maxene Musterfrau",
            "var_archiver_name": "Eric Blanc",
            "var_client_name": "Max Mustermann",
            "var_SODAR_UUID": "00000000-0000-0000-0000-000000000000",
            "var_Gitlab_URL": "https://cubi-gitlab.bihealth.org",
            "var_start_date": "1970-01-01",
        }
        Config = collections.namedtuple("Config", config)
        config = Config(**config)

        readme_path = os.path.join(tmp_dir, project_name, "README.md")
        cubi_tk.archive.readme.create_readme(readme_path, project_dir, config=config, no_input=True)
        f = open(readme_path, "r")
        for line in f.readlines():
            print(line.rstrip())
        f.close()

        assert cubi_tk.archive.readme.is_readme_valid(readme_path)
