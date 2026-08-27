#!/usr/bin/env bash

outdir='/tmp/fuzzydatatest_3/'
nc=20
nr=10000
nv=10

# NOTE: pivot is excluded during *pandas* generation on purpose. This workflow is replayed
# against the sql client below, and SQLOperation cannot express a generic pivot. Each client
# declares its own unsupported_ops, which generate_workflow applies automatically -- but that
# only covers generating *with* that client, not replaying someone else's workflow on it. For
# cross-client replay the generating client must restrict itself to the shared subset.
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