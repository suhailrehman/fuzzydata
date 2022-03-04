[![Build Status](https://app.travis-ci.com/suhailrehman/fuzzydata.svg?token=t8U2hzgp1btUxBBFMtEf&branch=main)](https://app.travis-ci.com/suhailrehman/fuzzydata)
[![codecov](https://codecov.io/gh/suhailrehman/fuzzydata/branch/main/graph/badge.svg?token=MA1BZQ60JB)](https://codecov.io/gh/suhailrehman/fuzzydata)
[![Twitter URL](https://img.shields.io/twitter/url/https/twitter.com/fold_left.svg?style=social&label=Follow%20%40suhailrehman)](https://twitter.com/suhailrehman)
[![PyPI version](https://badge.fury.io/py/fuzzydata.svg)](https://badge.fury.io/py/fuzzydata)


![fuzzydata](https://raw.githubusercontent.com/suhailrehman/fuzzydata/main/docs/logo.png)
---------------------------
# The fuzzydata Workflow Generator

The `fuzzydata` workflow generator enables:

* Abstract specification of Dataframe-based Workflows
* Generation of randomized tables and workflows 
* Loading and replay of workflows on multiple clients

Fuzzydata is designed to run on:

* [`pandas`]()
* [`modin[dask|ray]`]()
* [`SQLIte`]()

`fuzzydata` is designed to be extensible, you may implement your own client. 
Please see the existing clients in [fuzzydata/clients](https://github.com/suhailrehman/fuzzydata/tree/main/fuzzydata/clients) for ways to extend the abstract `Artifact`, `Operation`
and `Workflow` classes for your client.

## Installation

Manual build/install using pip:
```bash
pip install fuzzydata
```

## Usage

Some examples of fuzzydata usage are in the `examples` directory. You can also run the `fuzzydata` command 
to get a list of command-line options supported in fuzzydata

```
$ fuzzydata --help
usage: fuzzydata [-h] [--wf_client WF_CLIENT] [--output_dir OUTPUT_DIR] [--wf_name WF_NAME]
              [--columns COLUMNS] [--rows ROWS] [--versions VERSIONS] [--bfactor BFACTOR]
              [--matfreq MATFREQ] [--npp NPP] [--log LOG] [--replay_dir REPLAY_DIR]
              [--wf_options WF_OPTIONS] [--exclude_ops EXCLUDE_OPS] [--scale_artifact SCALE_ARTIFACT]

optional arguments:
  -h, --help            show this help message and exit
  --wf_client WF_CLIENT
                        Workflow Client to be used (Default pandas). Available Workflows: pandas|modin|sql
  --output_dir OUTPUT_DIR
                        Location of Output datasets to be stored
  --wf_name WF_NAME     prefix for each workflow to be generated dir to be the path prefix for these files.
  --columns COLUMNS     Number of columns in the base version
  --rows ROWS           Number of rows in the base version
  --versions VERSIONS   Number of artifact versions to generate
  --bfactor BFACTOR     Workflow Branching factor, 0.1 is linear, 100 is star-like
  --matfreq MATFREQ     Materialization frequency, i.e. how many operations before writing out an artifact
  --log LOG             Set Logging Level
  --replay_dir REPLAY_DIR
                        Replay existing workflow in directory
  --wf_options WF_OPTIONS
                        JSON-encoded workflow engine options like sql_string or modin_engine
  --exclude_ops EXCLUDE_OPS
                        JSON-encoded list of ops to exclude e.g. ["pivot"]
  --scale_artifact SCALE_ARTIFACT
                        JSON-encoded dict of {artifact_label: new_size} to be scaled up e.g. {"artifact_0"
                        : 1000000}
```

# License
[MIT License](https://github.com/suhailrehman/fuzzydata/blob/main/LICENSE)

# Contributing to fuzzydata
Check out the current roadmap in  [docs/roadmap.md](https://github.com/suhailrehman/fuzzydata/blob/main/docs/roadmap.md). You are always welcome to develop a new client for
fuzzydata.

# Contact
[Suhail Rehman](https://www.suhailrehman.com) / [ChiData Group](https://data.cs.uchicago.edu) @ [Uchicago CS](https://cs.uchicago.edu/)