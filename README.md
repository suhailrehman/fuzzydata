[![build](https://github.com/suhailrehman/fuzzydata/actions/workflows/run_tests.yml/badge.svg)](https://github.com/suhailrehman/fuzzydata/actions/workflows/run_tests.yml)
[![codecov](https://codecov.io/gh/suhailrehman/fuzzydata/graph/badge.svg?token=MA1BZQ60JB)](https://codecov.io/gh/suhailrehman/fuzzydata)
[![PyPI version](https://badge.fury.io/py/fuzzydata.svg)](https://badge.fury.io/py/fuzzydata)
[![Downloads](https://pepy.tech/badge/fuzzydata)](https://pepy.tech/project/fuzzydata)
[![Chidata Group](https://img.shields.io/badge/-chidata-white?link=https://data.cs.uchicago.edu&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAYAAAAf8/9hAAAB90lEQVQ4jZWTTUiUcRDGf/N+lAutVlsI2YLurq4sBF7zFIQnu1QkkQVt20LHpA4Rtm2EnYSILlEq0iFika4duhREhy5R4de6mGWYF7UgxGrf/3Twpf1wV+i5zcwz88wzMEId3IBDNqx8iYcx4obGZ+Y/1uJJdSILMSArcFqhe7GjDRF5g/C0aJmr41MLy+V8p6zx4KYwScDVShkb6LeLTJxrbn6xMxjcM1IofAVwsrAPuC5wSSHgt6waGLbggxjtwiot6uyy92Kb2VRn6wPb/L7jGDhlwYCv+BO4BwxPJxLrQW/jgiUM1jAeEKwBIw1zTlXp8CRMN3a0nWnyNm4iRP28QXVCxXsL2DVvAPAHfjTFW48h8thPqSrP8bzB0cLndwDJ6IFw3QGbHYj4lhXOjs7OP9lioQzWdkWBk+cjkfb/GmApC6Df/PCEs4PJVGfbw2Q0Gq7mbhngQrJlbiH/a229XZVriq4CriBp29X8xXjkru02pCq2zMBxC3KU7rEEDAEjS5FIwLh6BZHLQLBKvGiM9NmvYOYo5BT2AwmgEegF+oNra8uLK9/vu7tDj8TyHFHpQrBRzRnoG8vPv674hSx0AbcFerX0J1MGMrfgWToWazGWFxrLf3r/z0Ktw2ShW2FI4AiAwkuBdBYKtfh1kYGeDPRsx/kLmNqfY7ERV/wAAAAASUVORK5CYII=)](https://data.cs.uchicago.edu)
[![Twitter URL](https://img.shields.io/twitter/url/https/twitter.com/fold_left.svg?style=social&label=Follow%20%40suhailrehman)](https://twitter.com/suhailrehman)


![fuzzydata](https://raw.githubusercontent.com/suhailrehman/fuzzydata/main/docs/logo.png)
---------------------------
# The fuzzydata Workflow Generator

The `fuzzydata` workflow generator enables:

* Abstract specification of Dataframe-based Workflows
* Generation of randomized tables and workflows
* **Reproducible generation** from a seed, including the realized randomness of stochastic
  operators, so a serialized workflow genuinely replays
* Generation from **real seed tables** rather than only synthetic ones
* Loading and replay of workflows on multiple clients
* **Ground-truth lineage** with per-edge provenance annotations and equivalence classes over
  mutually-derivable artifacts — see [docs/lineage_corpus.md](docs/lineage_corpus.md)

Fuzzydata is currently designed to run using the following *clients*:

* [`pandas`](https://pandas.pydata.org/)
* [`SQLite`](https://www.sqlite.org/index.html)
* [`modin[dask|ray]`](https://modin.readthedocs.io/en/stable/) — **deprecated as of 0.1.0**,
  see below

`fuzzydata` is designed to be extensible, you may implement your own client. 
Please see the existing clients in [fuzzydata/clients](https://github.com/suhailrehman/fuzzydata/tree/main/fuzzydata/clients) for ways to extend the abstract `Artifact`, `Operation`
and `Workflow` classes for your client.

## Installation

Manual build/install using pip. 
```bash
pip install fuzzydata
```

`SQLAlchemy` is a required dependency and is always installed. The only optional extra is
`modin`:
```bash
pip install fuzzydata[modin]
```

> **The `modin` client is deprecated as of 0.1.0 and is not covered by CI.** It still ships
> and still works, but is unsupported. `ModinWorkflow` starts a dask (or ray) cluster on
> construction, which is impractical to test in CI, and the client is a thin subclass of the
> pandas client. Prefer `pandas` or `sql`.

## Usage

Some examples of fuzzydata usage are in the `examples` directory. You can also run the `fuzzydata` command 
to get a list of command-line options supported in fuzzydata

```
$ fuzzydata --help
usage: fuzzydata [-h] [--wf_client WF_CLIENT] [--output_dir OUTPUT_DIR]
                 [--wf_name WF_NAME] [--columns COLUMNS] [--rows ROWS]
                 [--versions VERSIONS] [--bfactor BFACTOR] [--matfreq MATFREQ]
                 [--format {csv,parquet}] [--base_artifact BASE_ARTIFACT]
                 [--seed SEED]
                 [--topology {chain,star,balanced,random_recursive,bfactor}]
                 [--log LOG] [--replay_dir REPLAY_DIR]
                 [--wf_options WF_OPTIONS] [--exclude_ops EXCLUDE_OPS]
                 [--scale_artifact SCALE_ARTIFACT]

options:
  -h, --help            show this help message and exit
  --wf_client WF_CLIENT
                        Workflow Client to be used (Default pandas). Available
                        Workflows: pandas|sql|modin
  --output_dir OUTPUT_DIR
                        Location of Output datasets to be stored
  --wf_name WF_NAME     prefix for each workflow to be generated dir to be the
                        path prefix for these files.
  --columns COLUMNS     Number of columns in the base version
  --rows ROWS           Number of rows in the base version
  --versions VERSIONS   Number of versions to generate
  --bfactor BFACTOR     Workflow branching factor. Parent selection weights
                        artifacts as exp(bfactor * index), so LOW values
                        spread new artifacts across all existing ones
                        (branchy) while HIGH values almost always extend the
                        newest artifact (a chain). Note this is the reverse of
                        what releases up to 0.0.11 documented.
  --matfreq MATFREQ     Materialization frequency, i.e. how many operations
                        before writing out an artifact
  --format {csv,parquet}
                        Artifact serialization format. parquet preserves
                        dtypes (csv round-trips through text) and is faster at
                        corpus scale.
  --base_artifact BASE_ARTIFACT
                        Path to a real seed table (.csv or .parquet) to use as
                        the base artifact instead of generating one with
                        Faker. Its schema is profiled from the data.
                        --columns/--rows are ignored.
  --seed SEED           Integer seed for reproducible generation. The same
                        seed yields the same graph, operations and artifact
                        contents. Omit for nondeterministic generation.
  --topology {chain,star,balanced,random_recursive,bfactor}
                        Parent-selection strategy:
                        chain|star|balanced|random_recursive|bfactor.
                        'bfactor' (default) uses the exponential weighting
                        controlled by --bfactor; the others select
                        deterministically. Only these produce a true star or
                        chain -- exponential weighting cannot.
  --log LOG             Set Logging Level
  --replay_dir REPLAY_DIR
                        Replay existing workflow in directory
  --wf_options WF_OPTIONS
                        JSON-encoded workflow engine options like sql_string
                        or modin_engine
  --exclude_ops EXCLUDE_OPS
                        JSON-encoded list of ops to exclude e.g. ["pivot"]
  --scale_artifact SCALE_ARTIFACT
                        JSON-encoded dict of {artifact_label: new_size} to be
                        scaled up e.g. {"artifact_0" : 1000000}
```

## Transformations

| operator | category | notes |
|---|---|---|
| `apply` | map | linear `ax + b` on a numeric column; adds a derived column |
| `astype` | map | type coercion; only widening casts are generated |
| `fill` | map | replace a value that actually occurs in the column |
| `label_encode` | map | categorical values to integer codes |
| `normalize` | map | min-max scale into `[0, 1]` |
| `standardize` | map | zero-mean, unit-variance scale |
| `project` | projection | keep a subset of columns |
| `rename` | projection | metadata only, so losslessly invertible |
| `select` | selection | row filter on a numeric threshold |
| `dropna` | selection | drop rows with nulls |
| `dedupe` | selection | drop duplicate rows |
| `sample` | sampling | fractional row sample; records its `random_state` |
| `train_test_split` | sampling | two complementary siblings sharing a seed |
| `groupby` | aggregation | group and aggregate |
| `pivot` | reshaping | wide reshape; chain-final |
| `one_hot_encode` | reshaping | one indicator column per recorded category |
| `merge` | augmenting | join against a synthesised PK-FK table |

Not every client supports every operator. Each declares what it cannot express in
`Operation.unsupported_ops`, and the generator excludes those up front rather than failing
mid-run. The SQL client currently excludes `pivot`, `normalize`, `standardize`,
`label_encode` and `train_test_split`.

## Generating a lineage corpus

```bash
python scripts/generate_corpus.py --output_dir /data/corpus \
    --num_workflows 200 --base_seed 20260901 \
    --seed_table_dir /data/seed_tables --distractor_pool 50 --workers 8
```

The same `--base_seed` reproduces the same corpus, across worker counts and across separate
invocations. See [docs/lineage_corpus.md](docs/lineage_corpus.md) for the full guide and
[docs/topology.md](docs/topology.md) for the graph-shape options.

# Documentation
Download our paper [here](http://people.cs.uchicago.edu/~suhail/publication/rehman-fuzzydata-2022/rehman-fuzzydata-2022.pdf).

If you use fuzzydata in your research, please consider citing our paper:
```
@inproceedings{10.1145/3531348.3532178,
author = {Rehman, Mohammed Suhail and Elmore, Aaron},
title = {FuzzyData: A Scalable Workload Generator for Testing Dataframe Workflow Systems},
year = {2022},
isbn = {9781450393539},
publisher = {Association for Computing Machinery},
address = {New York, NY, USA},
url = {https://doi.org/10.1145/3531348.3532178},
doi = {10.1145/3531348.3532178},
booktitle = {Proceedings of the 2022 Workshop on 9th International Workshop of Testing Database Systems},
pages = {17–24},
numpages = {8},
location = {Philadelphia, PA, USA},
series = {DBTest '22}
}
```

# License
[MIT License](https://github.com/suhailrehman/fuzzydata/blob/main/LICENSE)

# Contributing to fuzzydata
Check out the current roadmap in  [docs/roadmap.md](https://github.com/suhailrehman/fuzzydata/blob/main/docs/roadmap.md). You are always welcome to develop a new client for
fuzzydata.

# Contact
[Suhail Rehman](https://www.suhailrehman.com) / [ChiData Group](https://data.cs.uchicago.edu) @ [Uchicago CS](https://cs.uchicago.edu/)
