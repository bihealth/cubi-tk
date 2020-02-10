# History

## HEAD (unreleased)

- Adjusting package meta data in `setup.py`.
- Fixing documentation bulding bug.
- Documentation is now built during testing.
- Adding `cubi-sak snappy pull-sheet`.
- Converting `snappy-transfer_utils`, adding `cubi-sak snappy ...`
    - `itransfer-raw-data`
    - `itransfer-ngs-mapping`
    - `itransfer-variant-calling`
- Adding `mypy` checks to CI.
- Adding `--dry-run` and `--show-diff` arguments to `cubi-sak snappy pull-sheet`.
- Adding `cubi-sak snake check` command.
- Adding `cubi-sak isa-tab validate` command.
- Adding `cubi-sak isa-tab resolve-hpo` command.
- Adding `cubi-sak sodar download-sheet` command.
- Adding `cubi-sak snappy kickoff` command.
- Adding `cubi-sak org-raw {check,organize}` command.
- `cubi-sak snappy pull-sheet` is a bit more interactive.
- Adding `cubi-sak sea-snap pull-isa` command.
- Adding `cubi-sak sea-snap write-sample-info` command.
- Adding `cubi-sak sea-snap itransfer-mapping-results` command.

## v0.1.0

- Bootstrapping `cubi-sak` with ISA-tab templating via `cubi-sak isa-tpl <tpl>`.
