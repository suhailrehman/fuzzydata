# Changelog

All notable changes to this project are documented here.

This project follows [Semantic Versioning](https://semver.org/). While the version is
`0.x`, the public API is unstable and breaking changes may land in minor releases.

## [0.1.0] - 2026-08-27

Adds everything needed to generate a corpus for content-only data lineage inference:
reproducible generation, real seed tables, an ML-shaped operator library, named graph
topologies, per-edge provenance annotations, equivalence classes, and a corpus driver.

The headline behaviour fix is that **a serialized workflow now actually replays**. Through
0.0.11 `sample` emitted `.sample(frac=...)` with no `random_state`, so the JSON spec
advertised a replayability it did not have.

### Added

- **Reproducible generation.** `generate_workflow(seed=...)` and `--seed`. All randomness
  flows through an explicit `np.random.Generator`; Faker is seeded per call. Stochastic
  operators draw a concrete `random_state`, emit it into the generated code and persist it in
  the operation record.
- **Real seed tables.** `base_artifact=` / `--base_artifact` accept a `.csv` or `.parquet`
  table. `fuzzydata.lineage.profiler` profiles it into fuzzydata's column-type labels;
  integer primary keys are deliberately not labelled numeric. `InsufficientSchemaError` names
  the missing label when a table admits no legal operations.
- **Nine operators**: `dropna`, `dedupe`, `rename`, `astype`, `normalize`, `standardize`,
  `label_encode`, `one_hot_encode`, `train_test_split`. `apply`, `select` and `fill` were
  already implemented but commented out of the generator and are now generatable, so the
  corpus contains a row-level map operation for the first time. Fifteen operators are
  generated, up from five.
- **`Operation.unsupported_ops`**, a declarative per-client capability registry, applied by
  the generator up front. Previously `--wf_client=sql` crashed on default flags unless the
  caller knew to pass `exclude_ops=["pivot"]`.
- **Named topologies.** `topology=` / `--topology` accepting `chain`, `star`, `balanced`,
  `random_recursive`, `bfactor`. No value of `bfactor` produces a true star, so the star
  stratum was previously ungeneratable. Measured shapes in `docs/topology.md`.
- **Per-edge provenance annotations** (`fuzzydata.lineage.annotations`): category, stochastic,
  augmenting, invertible_on_input, realized composition_depth, sibling_group.
- **Equivalence classes** (`fuzzydata.lineage.equivalence`), emitted as
  `{name}_equivalence_classes.json`. Two artifacts share a class iff joined by invertible
  edges. Invertibility is computed from data where it depends on data.
- **Corpus driver**: `fuzzydata.lineage.corpus` and `scripts/generate_corpus.py`, with a
  parameter grid, a deterministically sorted `manifest.json`, a distractor pool for
  negatives, and multiprocessing fan-out. Reproducible across worker counts and invocations.
- **Idiom policy**: `operator_policy='idiom'` and `fuzzydata.lineage.idioms`. The default
  `schema_constrained` policy makes each edge conditionally independent of its predecessors
  given the schema, so it is itself a test of the state-independence assumption; `idiom` is
  the correlated alternative.
- **Artifact-validity gate** (`fuzzydata.lineage.validity`) plus generator pre-conditions.
  Degenerate artifacts fell from 11% to 2% of a measured 600.
- **parquet artifacts** via `file_format=` / `--format`, with `pyarrow` now a hard dependency.
- `docs/lineage_corpus.md`, `docs/topology.md`, `CITATION.cff`, this changelog.
- `fuzzydata-corpus` console script. The corpus CLI lives in
  `fuzzydata.lineage.corpus_cli` rather than in `scripts/`, because the legacy setup.py
  `scripts=` list only carried the main CLI -- `scripts/generate_corpus.py` was absent from
  the sdist and wheel entirely, which the release dry run caught.

### Fixed

- **`sample` did not record its randomness**, so replay could not reproduce the original
  artifacts. See Added, above.
- **Cross-process nondeterminism.** `generator._faker_cols` was built with
  `list(set(...))` over provider names; string set order is randomised per interpreter, so
  the same seed produced different schemas in a new process. Invisible to same-process tests
  and to multiprocessing tests alike, since forked workers inherit the parent's hash seed.
- **Merges were silently dropped at `matfreq=1`.** The `force_materialize` break jumped out
  of the chain loop before the operation counter incremented, so the guard below skipped
  materialization: the synthesised right-hand table was added to the workflow and the join
  never produced. The corpus gained an orphan parentless artifact and lost the merge edge.
- **Serialization wrote the dataframe index**, so every round-trip injected an `Unnamed: 0`
  column: replayed artifacts differed from the originals and every artifact on disk carried a
  phantom, perfectly-correlated column. `to_sql` was inconsistent in the same way.
- **`Operation` aliased the source artifact's `schema_map`** instead of copying it. `apply` is
  the only operator that inserts a key rather than rebuilding the dict, so it mutated the
  parent's recorded schema in place; the parent then advertised a column it did not contain.
- **`pivot` left artifacts with an empty schema map** forever — a permanent dead end for
  further generation and a hole in the ground truth. Now recovered by profiling the
  materialized result. Also, `pivot_table` returns a `MultiIndex` on the columns, which
  cannot survive a CSV round-trip and which no downstream operator can address; results are
  flattened. Together these are why `topology='chain'` could not reach its own definition:
  measured depth went from 6 to 19 for `n=20`.
- **`--bfactor 100` crashed.** The leading constant `bfactor/(exp(bfactor*size)-1)` overflowed
  to `0` and then to `nan` probabilities. It is cancelled by the following normalisation, so
  it was removed. The CLI default (`5.0`) also disagreed with the function default (`0.5`).
- **`label_encode` changed a column's values without updating its recorded type**, so a later
  `fill` faked a same-provider string into what was now an integer column. The resulting
  mixed-type column cannot be written to parquet at all. `astype` and `one_hot_encode` had
  the same flaw.
- **`apply`'s derived column name disagreed across three files**, and the pandas
  implementation emitted `.assign(name = lambda x: x.col ...)`, which is a `SyntaxError` when
  a column label starts with a digit — as ~16% do, since prefixes are drawn from
  `ascii_letters + digits`.
- **`fill` arguments were pre-quoted for the pandas `eval()` path**, which the SQL client then
  double-quoted into a syntax error. Arguments are raw; each client quotes for its own target.
  The SQL `fill` also excluded columns with `set(col_name)` — the set of the string's
  characters — so the column was never actually excluded.
- **Mixed-type columns broke sorting.** `one_hot_encode` categories and merge keys were
  sorted assuming homogeneous types.
- **The shipped example specs could not be replayed** (`KeyError('op_list')`): both used
  `operation_list` as the inner per-operation key.
- **`pivot` was near-ungeneratable.** `generate_workflow` appended to a mutable default
  `exclude_ops=[]` inside the chain loop, so the exclusion accumulated without bound and
  leaked across calls in one process. `wf_options={}` had the same defect.
- **SQL workflows could not be replayed at all.** `SQLOperation.chain_operation` never called
  `super()`, so every SQL spec serialized with an empty `op_list`.
- **Nine tests had never executed.** `pytest-dependency` was used with static dependency
  names, which never resolve for parametrized tests and cause a silent skip — so
  serialization, deserialization and replay were entirely untested. `pytest-dependency` was
  also missing from `requirements.txt`.
- **Test fixtures corrupted each other.** The `*_static` and `*_generated` artifact fixtures
  both called `.generate()` on one shared session-scoped object, so whichever was
  instantiated first won.
- Artifact filenames were hardcoded to `.csv` at three creation sites while serialization used
  `file_format`, so any other format desynchronised the two. `load_workflow` now detects the
  format on disk instead of assuming csv.

### Changed

- `python_requires` raised to `>=3.9`, matching what CI actually tests; 3.7 and 3.8 were
  claimed but never exercised. Per-version classifiers added.
- The **modin client is deprecated** and excluded from the test suite; `ModinWorkflow` emits a
  `DeprecationWarning`. It still ships and still works. `requirements.txt` no longer installs
  `modin[all]`, which pulled in both dask and ray.
- CI moved fully to GitHub Actions; `.travis.yml` deleted. Tests run on Python 3.9 and 3.13.
  The release workflow previously targeted the retired `ubuntu-20.04` runner label and could
  never have scheduled; it now builds with `python -m build` and publishes via PyPI Trusted
  Publishing.
- `fuzzydata.clients.travis_workflows` renamed to `core_workflows`.
- `pytest.ini` no longer enables `log_cli`.
- The `--bfactor` help and README description were corrected; they had the direction backwards
  through 0.0.11.

### Removed

- `.travis.yml` and the committed `.idea/` directory.

### Security

- A codecov upload token was committed in `.travis.yml`. Deleting the file does not revoke it
  and it remains in the git history: **rotate it.**
