# Generating a lineage-inference corpus

End-to-end guide to producing a corpus for content-only data lineage inference: many
workflows, each with ground-truth lineage, per-edge provenance metadata, and equivalence
classes over mutually-derivable artifacts.

## Quick start

```bash
pip install fuzzydata   # pyarrow is included, for parquet artifacts

python scripts/generate_corpus.py \
    --output_dir /data/corpus \
    --num_workflows 200 \
    --base_seed 20260901 \
    --seed_table_dir /data/seed_tables \
    --distractor_pool 50 \
    --format parquet \
    --workers 8
```

Inspect the plan without generating anything:

```bash
python scripts/generate_corpus.py --output_dir /tmp/x --num_workflows 200 \
    --base_seed 20260901 --plan_only | head -40
```

## Reproducibility

The same `--base_seed` and grid reproduce the same corpus, **including across different
worker counts and separate invocations**. Every workflow's seed is derived from the base seed
and the workflow index alone, via `numpy` `SeedSequence` spawn keys — never from process id,
clock, or completion order. The manifest is sorted by workflow id, because multiprocessing
completion order varies.

Two footguns this had to survive, both of which are easy to reintroduce:

- **Consecutive integer seeds produce correlated streams.** Spawn keys exist to avoid this;
  `base_seed + index` would not be good enough.
- **`set` iteration over strings is randomised per process.** Any `list(set(...))` of
  provider or column names makes the same seed produce different output in a new process.
  This shipped once, in `generator._faker_cols`, and was invisible to same-process and
  multiprocessing tests alike — forked workers inherit the parent's hash seed. There is a
  subprocess test guarding it now.

## Use real seed tables

`--seed_table_dir` is the highest-value option here. Faker-generated base artifacts have no
inter-column correlation, no functional dependencies and no semantic column names, so an
encoder trained only on them learns features that do not transfer to real tables.

Seed tables are `.csv` or `.parquet`, assigned round-robin across workflows. Each is profiled
into fuzzydata's column-type labels (`numeric`, `groupable`, `joinable`, `string`) by
`fuzzydata.lineage.profiler`. Note that an integer primary key is deliberately **not**
labelled numeric: arithmetic and aggregation over ids are meaningless and would teach a
spurious signal.

A table that admits no legal operations raises `InsufficientSchemaError` naming what is
missing, rather than failing deep inside the generator.

## What a workflow directory contains

```
wf_00007/
├── artifacts/                       # one file per artifact, .csv or .parquet
├── wf_00007_operations.json         # metadata + operation records (the spec; replayable)
├── wf_00007_gt_graph.csv            # ground-truth lineage graph, networkx edgelist
├── wf_00007_schema_map.json         # column -> type label, per artifact
├── wf_00007_equivalence_classes.json# artifact -> class id
├── wf_00007_perf.csv                # per-operation timings
└── wf_00007_code.py                 # standalone runnable pandas script
```

### Operation records

Each entry in `operation_list` records the edge and its provenance:

```json
{
  "sources": ["artifact_3"],
  "new_label": "artifact_4",
  "op_list": [{"op": "sample", "args": {"frac": 0.62, "random_state": 3172841}}],
  "annotation": {
    "composition_depth": 1,
    "categories": ["sampling"],
    "stochastic": true,
    "augmenting": false,
    "invertible_on_input": false,
    "sibling_group": null,
    "ops": [{"op": "sample", "category": "sampling", "stochastic": true,
             "augmenting": false, "chain_position": 0}],
    "unrecognized_ops": []
  }
}
```

- `random_state` is the **realized** randomness. Ground truth is defined conditional on it,
  and without it a serialized workflow cannot be replayed — which was true of `fuzzydata`
  through 0.0.11 despite the spec implying otherwise.
- `composition_depth` is the realized chain length, not the configured `matfreq`. `matfreq`
  is an upper bound: a `merge` forces materialization and `pivot` is chain-final.
- `sibling_group` links the two halves of a `train_test_split`. They share a `random_state`
  and are **complementary** — train takes the sampled rows, test takes the remainder — so
  together they partition the parent. `Operation` is single-destination, so a two-output
  operator has to be modelled this way for now.
- `unrecognized_ops` should always be empty. A non-empty value means an operator reached the
  corpus without a category, which the test suite treats as a failure.

### Equivalence classes

Lineage is identifiable only up to mutual derivability: if A can be recovered from B and B
from A, no content-based method can tell which came first. `*_equivalence_classes.json` maps
each artifact to a class id; two artifacts share a class iff joined by a path of invertible
edges. Compute quotient-aware metrics over these classes, not over raw artifacts.

Invertibility is decided symbolically where possible and computed from data where not:

| operator | invertible |
|---|---|
| `rename` | yes — metadata only |
| `apply` | iff `a != 0` |
| `astype` | iff the cast widens (`int -> float` yes, `float -> int` no) |
| `fill` | iff the replacement value did not already occur in the column |
| everything else | no |

`one_hot_encode` is treated as non-invertible on purpose. It is information-preserving when
every row carries exactly one recorded category, but nulls and unseen values are dropped
silently and that is not recorded. Over-claiming invertibility merges artifacts that are not
equivalent, which corrupts the metrics worse than splitting classes too finely.

## Negatives

`--distractor_pool N` adds N single-artifact workflows with no derivations. Without them
every candidate pair in the corpus is genuinely related, and precision cannot be measured at
all.

## Operator policy and the state-independence assumption

`operator_policy='schema_constrained'` (the default) picks uniformly among the operations the
schema permits. That makes each edge conditionally independent of its predecessors given the
schema — so **the default is itself a test of the state-independence assumption, not a
neutral choice.**

`operator_policy='idiom'` is the correlated alternative. It samples a latent workflow idiom
per workflow (`ml_prep`, `bi_rollup`, `eda`, `enrich`) and biases selection toward that
idiom's next stage, subject to schema legality. Adherence is 0.75, so idiom workflows still
contain off-idiom edges — a perfectly obedient idiom would be trivially identifiable and
would test nothing. The idiom is recorded in workflow metadata for stratification.

Run the ablation by generating both policies from the same base seed and comparing.

## Artifact validity

Chained selection can drive an artifact to a handful of rows; `project` can reduce it to one
column; `groupby` collapses cardinality. Such artifacts teach a content encoder nothing and
are mutually indistinguishable regardless of their true parent, putting a floor on achievable
accuracy that has nothing to do with the method under test.

The generator pre-empts the main causes (it will not offer a `groupby` whose group count is
below the row floor, an over-wide or too-short `pivot`, a single-column `project`, or a
`sample` that would shrink past the floor). Those pre-conditions are approximate when
`matfreq > 1`, because the generator only sees the artifact at the *start* of a chain.
Measured residual: about 2% of artifacts, down from 11% before the guards.

`fuzzydata.lineage.validity.validate_workflow()` is therefore also a post-hoc gate, and every
manifest row carries a `validity` summary. Use `generate_workflow(validate='strict')` to
refuse degenerate workflows outright.

## Choosing a scale

State the target explicitly before generating; it is the number most likely to bite.

```
disk ≈ num_workflows × num_versions × rows × columns × bytes_per_cell
```

As a worked example, 200 workflows × 10 artifacts × 10k rows × 10 columns at roughly 8
bytes/cell is about 16 GB in parquet, materially more in csv. Start with
`--num_workflows 5 --plan_only`, then a small real run, and measure before committing to the
full grid.

## Format

`--format parquet` is recommended and is the corpus script's default. csv round-trips
everything through text, so dtypes survive only by inference — an int column comes back as
int only by luck. parquet is also substantially faster at scale.

One consequence worth knowing: parquet refuses to write a mixed-type object column, where csv
silently accepts it. That surfaced a real bug (`label_encode` changing a column's values to
integer codes without updating its recorded type, so a later `fill` faked a string into an
integer column). If you add an operator that changes a column's type, update the schema map
in the same place — `tests/test_corpus.py::test_no_mixed_type_object_columns` guards it.
