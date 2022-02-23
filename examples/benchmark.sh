#!/usr/bin/env bash

outdir='/tmp/fuzzydatatest_3/'
nc=20
nr=10000
nv=10

python ../fuzzydata/cli.py --wf_client=pandas \
                            --output_dir=$outdir/pandas/ \
                            --columns=$nc --rows=$nr --version=$nv \
                            --exclude_ops='["pivot"]'

python ../fuzzydata/cli.py --wf_client=sql \
                           --replay_dir=$outdir/pandas/ \
                           --output_dir=$outdir/sqlite/ \


python ../fuzzydata/cli.py --wf_client=modin \
                            --output_dir=$outdir/modin_dask/ \
                            --replay_dir=$outdir/pandas/ \
                            --wf_options='{"modin_engine": "dask"}'

python ../fuzzydata/cli.py --wf_client=modin \
                            --output_dir=$outdir/modin_ray/ \
                            --replay_dir=$outdir/pandas/ \
                            --wf_options='{"modin_engine": "ray"}'