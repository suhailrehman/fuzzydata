# Changelog

All notable changes to this project are documented here.

This project follows [Semantic Versioning](https://semver.org/). While the version is
`0.x`, the public API is unstable and breaking changes may land in minor releases.

## [0.1.1] - 2026-08-27

A focused maintenance and corpus-enhancement release. No breaking changes.

### Added

- **`sort` and `shuffle` operators** (`category='order'`). `sort` accepts a column list and an
  `ascending` flag; `shuffle` draws a concrete `random_state` for reproducibility. Both are
  invertible and are included in the equivalence-class graph. `shuffle` is the only stochastic
  member of the new category. SQLite marks both as unsupported.
- **`invertible_bias` knob** on `generate_workflow`, both CLIs, and the corpus grid. When > 0,
  operators whose application is invertible on the current artifact are up-weighted before
  uniform sampling, increasing the share of artifacts in non-trivial equivalence classes without
  changing the operator set. The bias value is recorded in `wf.metadata` and in the manifest
  row. Implemented via `_predict_invertible()`, a schema-level mirror of `_step_is_invertible`
  that does not require a materialized `Operation` object.
- **Reversal-closed workflow pairs** (`fuzzydata.lineage.reversal`). `generate_reversal_pair()`
  produces two workflows over the same set of artifact DataFrames with all provenance edges
  flipped. Because every operator in `REVERSAL_CLOSED_OPS` (`sort`, `shuffle`, `rename`,
  `apply`) is invertible, both directions represent valid derivations, making the pair a
  construction for testing orientation sensitivity. `generate_corpus` gains a `reversal_pool`
  parameter; each pair contributes two manifest rows tagged `kind='reversal_closed'` with a
  shared `reversal_pair_id`.
- **`topology='fitted'`** empirical distribution steering. `generate_workflow` and both CLIs
  accept `topology_params` (a path to a JSON file or an inline dict) with `depth`,
  `branching_factor`, and `num_artifacts` discrete empirical distributions. Parent selection
  uses a two-phase heuristic: below the target depth it weights deeper artifacts higher to grow
  the longest path; after the target is reached it weights artifacts by spare branching
  capacity. The `num_artifacts` distribution overrides `num_versions`. The corpus grid gains
  `topology_params: [None]` by default.
- **Measured graph and equivalence fields in the manifest.** Each manifest row now includes
  `depth`, `max_out_degree`, `num_leaves`, `num_roots`, `invertible_edge_count`,
  `stochastic_edge_count`, `augmenting_edge_count`, `composition_depth_histogram`, and
  `class_size_histogram`, measured from the finished workflow at generation time.
- **`profiler.describe_table(df)`** returns a concise descriptor (`n_rows`, `n_cols`,
  `row_bucket`, `col_bucket`, `type_mix`, `admits_generation`) for any DataFrame. When a corpus
  workflow uses a real seed table the descriptor is written as `seed_descriptor` in its manifest
  row.
- **Environment block in `manifest.json`** recording the fuzzydata, Python, pandas, numpy,
  pyarrow, and networkx versions at generation time.
- **`corpus_cli.py` shipped as `fuzzydata-corpus`** (the existing `scripts/generate_corpus.py`
  was absent from the sdist and wheel because `setup.py scripts=` only carried the main CLI).

### Fixed

- **`_content_hash` raised `TypeError` under pandas 3.0.** `astype(str)` on a column
  containing `NA` preserves the `NA` as a float in pandas 3.0 rather than converting to
  `"nan"`. Switched to `pd.util.hash_pandas_object`, which is NA-safe and consistent across
  pandas 2.x and 3.x.
- **Merge `KeyError` in chain generation.** The generator tracks schema mutations
  (e.g. `rename` changes column names) in `current_schema_map` but reads the merge key from
  the source artifact's materialized DataFrame, which still carries the original names. A key
  drawn from the updated schema did not exist in the source and caused a `KeyError`. Fixed by
  checking that the key column exists in the source DataFrame before adding `merge` to
  `ops_choices`.
- **SQLAlchemy moved to `extras_require['sql']`**, with a new `extras_require['all']` that
  pulls in all optional dependencies. `pip install fuzzydata` no longer drags in SQLAlchemy for
  users who only use the pandas client.

### Changed

- Dependency version ranges tightened to ranges known to work: `pandas>=1.4.0,<3.1`,
  `numpy>=1.23.0,<2.1`, `pyarrow>=10.0.0,<26`, `faker>=13.3.0,<38`, `networkx>=2.7,<4`.

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
