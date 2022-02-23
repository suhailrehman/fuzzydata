#!/usr/bin/env bash

outdir='/tmp/fuzzydata_scaling_test_3/'


for scale in 1000 10000 100000 1000000 5000000;
do
  python ../fuzzydata/cli.py --wf_client=pandas \
                               --replay_dir=$outdir/original_${scale}/ \
                               --output_dir=$outdir/pandas_${scale}/ \


  python ../fuzzydata/cli.py --wf_client=sql \
                             --replay_dir=$outdir/original_${scale}/ \
                             --output_dir=$outdir/sqlite_${scale}/ \


  python ../fuzzydata/cli.py --wf_client=modin \
                              --output_dir=$outdir/modin_dask_${scale}/ \
                              --replay_dir=$outdir/original_${scale}/ \
                              --wf_options='{"modin_engine": "dask"}'

  python ../fuzzydata/cli.py --wf_client=modin \
                              --output_dir=$outdir/modin_ray_${scale}/ \
                              --replay_dir=$outdir/original_${scale}/ \
                              --wf_options='{"modin_engine": "ray"}'

done


