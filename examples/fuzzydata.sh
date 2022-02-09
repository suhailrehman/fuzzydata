#!/usr/bin/env bash

outdir='/tmp/fuzzydatatest/'
nc=20
nr=1000
nv=10
wf_client=modin


python ../fuzzydata/cli.py --wf_client=$wf_client \
                            --output_dir=$outdir \
                            --columns=$nc --rows=$nr --version=$nv \
