#!/usr/bin/env bash

outdir='/tmp/fuzzydata_scaling_test_3/'
nc=20
nr=1000
nv=15
client='pandas'

# Baseline Workflow
#python ../fuzzydata/cli.py --wf_client=$client \
#                            --output_dir=$outdir/pandas_1000/ \
#                            --columns=$nc --rows=$nr --version=$nv \
#                            --exclude_ops='["pivot"]'

for scale in 10000 100000 1000000 5000000;
do
  python ../fuzzydata/cli.py --wf_client=pandas \
                             --replay_dir=$outdir/original_1000/ \
                             --output_dir=$outdir/original_${scale}/ \
                             --scale_artifact='{"artifact_0": '$scale'}'
done


