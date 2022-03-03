#!/usr/bin/env bash

client=$1
indir=$2
outdir=$3

python ../fuzzydata/cli.py --wf_client=modin \
                            --replay_dir=$indir \
                            --output_dir=$outdir \
                            --wf_options='{"modin_engine": "ray"}'