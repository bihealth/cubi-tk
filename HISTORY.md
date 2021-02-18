# History

## HEAD (unreleased)

- temporarily working around SODAR REST API not returning sodar\_uuid where we expect it to
- using library\_ name as an alternative to folder\_name

## v0.2.0

- Adjusting package meta data in `setup.py`.
- Fixing documentation bulding bug.
- Documentation is now built during testing.
- Adding `cubi-tk snappy pull-sheet`.
- Converting `snappy-transfer_utils`, adding `cubi-tk snappy ...`
    - `itransfer-raw-data`
    - `itransfer-ngs-mapping`
    - `itransfer-variant-calling`
- Adding `mypy` checks to CI.
- Adding `--dry-run` and `--show-diff` arguments to `cubi-tk snappy pull-sheet`.
- Adding `cubi-tk snake check` command.
- Adding `cubi-tk isa-tab validate` command.
- Adding `cubi-tk isa-tab resolve-hpo` command.
- Adding `cubi-tk sodar download-sheet` command.
- Adding `cubi-tk snappy kickoff` command.
- Adding `cubi-tk org-raw {check,organize}` command.
- `cubi-tk snappy pull-sheet` is a bit more interactive.
- Adding `cubi-tk sea-snap pull-isa` command.
- Adding `cubi-tk sea-snap write-sample-info` command.
- Adding `cubi-tk sea-snap itransfer-mapping-results` command.
- Adding more tools for interacting with SODAR.
- Rebranding to `cubi-tk` / CUBI Toolkit

## v0.1.0

- Bootstrapping `cubi-tk` with ISA-tab templating via `cubi-tk isa-tpl <tpl>`.
