# Changelog

All notable changes to this project are documented here.

This project follows [Semantic Versioning](https://semver.org/). While the version is
`0.x`, the public API is unstable and breaking changes may land in minor releases.

## [Unreleased]

### Fixed

- **SQL workflows could not be replayed.** `SQLOperation.chain_operation` never invoked its
  superclass, so `Operation.op_list` was never populated. Every SQL workflow serialized with
  an empty `op_list` for every operation, meaning `<name>_operations.json` recorded no
  operations at all and replay produced nothing.
- **`pivot` was silently excluded from most generated workflows.** `generate_workflow` used a
  mutable default argument (`exclude_ops=[]`) and appended `'pivot'` to it inside the
  operation-chain loop. The exclusion accumulated without bound and leaked across calls in
  the same process, so after the first chained operation `pivot` could never be generated
  again. `wf_options={}` had the same mutable-default defect.
- **The `sql` client crashed on default settings.** Generating a SQL workflow required the
  caller to know to pass `exclude_ops=["pivot"]`; otherwise generation raised
  `NotImplementedError`. Clients now declare their own capabilities (see below).
- **Nine tests had never executed.** `pytest-dependency` was used with static dependency
  names, which never resolve for parametrized tests and cause a silent skip rather than an
  error. As a result serialization, deserialization and replay were entirely untested.
  `pytest-dependency` was also missing from `requirements.txt`.

### Added

- `Operation.unsupported_ops`, a declarative per-client set of operations a client cannot
  express. `generate_workflow` consults it automatically, so unsupported operations are
  excluded up front rather than discovered by raising mid-run. `SQLOperation` declares
  `pivot`.

### Changed

- **The modin client is deprecated** and is no longer covered by CI. It still ships and
  still works, but is unsupported: `ModinWorkflow` now emits a `DeprecationWarning`.
  `ModinWorkflow.__init__` starts a dask (or ray) cluster on construction, which is too
  heavy and too flaky to test in CI, and the client is a thin subclass of the pandas client
  that reuses `DataFrameOperation` verbatim. Run `FUZZYDATA_TEST_MODIN=1 pytest` to exercise
  it manually.
- `requirements.txt` no longer installs `modin[all]`, which pulled in both dask and ray.
  Install the `modin` extra explicitly if you need that client.
- CI moved fully to GitHub Actions; `.travis.yml` is deleted. Tests run on Python 3.9 and
  3.13. The release workflow now builds with `python -m build` and publishes via PyPI
  Trusted Publishing instead of a long-lived API token.
- `python_requires` raised to `>=3.9`, matching what is actually tested. 3.7 and 3.8 were
  previously claimed but never exercised.
- `fuzzydata.clients.travis_workflows` renamed to `core_workflows`.
- `pytest.ini` no longer enables `log_cli`, which emitted a log line per operation.

### Removed

- `.travis.yml` and the committed `.idea/` directory.

<!--
NOTE: the codecov token previously committed in .travis.yml must be rotated. Deleting the
file does not revoke it, and it remains in the git history.
-->
