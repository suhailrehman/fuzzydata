#!/usr/bin/env bash

outdir='/tmp/fuzzydatatest/nyc-cab/'
replay_dir='./nyc-cab/'



python ../../fuzzydata/cli.py --wf_client=pandas \
                           --replay_dir=$replay_dir \
                           --output_dir=$outdir/pandas/ \


#python ../../fuzzydata/cli.py --wf_client=sql \
#                           --replay_dir=$replay_dir \
#                           --output_dir=$outdir/sqlite/ \


python ../../fuzzydata/cli.py --wf_client=modin \
                            --output_dir=$outdir/modin_dask/ \
                            --replay_dir=$replay_dir \
                            --wf_options='{"modin_engine": "dask"}'

python ../../fuzzydata/cli.py --wf_client=modin \
                            --output_dir=$outdir/modin_ray/ \
                            --replay_dir=$replay_dir \
                            --wf_options='{"modin_engine": "ray"}'