#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Generate a lineage-inference corpus.

Example:

    python scripts/generate_corpus.py --output_dir /data/corpus \
        --num_workflows 200 --base_seed 20260901 \
        --seed_table_dir /data/seed_tables --distractor_pool 50 --workers 8

The same --base_seed and grid reproduce the same corpus regardless of --workers.
"""

import argparse
import glob
import json
import logging
import os
import sys

from fuzzydata.lineage.corpus import DEFAULT_GRID, generate_corpus

logger = logging.getLogger(__name__)


def _seed_tables(directory):
    if not directory:
        return []
    tables = sorted(glob.glob(os.path.join(directory, '*.csv'))
                    + glob.glob(os.path.join(directory, '*.parquet')))
    if not tables:
        logger.warning(f'No .csv or .parquet seed tables found in {directory}')
    return tables


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--output_dir', required=True,
                        help='Directory to write the corpus into')
    parser.add_argument('--num_workflows', type=int, default=10,
                        help='Number of lineage workflows to generate')
    parser.add_argument('--base_seed', type=int, default=0,
                        help='Root seed. Every workflow seed is derived from it, so the '
                             'same value reproduces the corpus exactly.')
    parser.add_argument('--seed_table_dir', default=None,
                        help='Directory of real seed tables (.csv/.parquet), assigned '
                             'round-robin. Without this, base artifacts are Faker-generated '
                             'and carry no inter-column correlation.')
    parser.add_argument('--distractor_pool', type=int, default=0,
                        help='Number of single-artifact workflows to add as negatives. '
                             'Without negatives every candidate pair is related and '
                             'precision cannot be measured.')
    parser.add_argument('--wf_client', default='pandas', choices=['pandas', 'sql'],
                        help='Workflow client')
    parser.add_argument('--columns', type=int, default=10,
                        help='Columns in generated base artifacts (ignored per workflow when '
                             'that workflow uses a seed table)')
    parser.add_argument('--rows', type=int, default=1000,
                        help='Rows in generated base artifacts')
    parser.add_argument('--format', default='parquet', choices=['csv', 'parquet'],
                        help='Artifact serialization format')
    parser.add_argument('--workers', type=int, default=None,
                        help='Process count. Does not affect the output.')
    parser.add_argument('--grid', default=None,
                        help='JSON object overriding the parameter grid, e.g. '
                             '\'{"matfreq": [1, 3], "topology": ["star"]}\'')
    parser.add_argument('--plan_only', action='store_true',
                        help='Print the workflow plan as JSON and exit without generating')
    parser.add_argument('--log', default='info', help='Logging level')

    options = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, options.log.upper(), logging.INFO),
                        format='%(asctime)s %(levelname)s %(name)s: %(message)s')

    grid = json.loads(options.grid) if options.grid else None
    unknown = set(grid or {}) - set(DEFAULT_GRID)
    if unknown:
        parser.error(f'Unknown grid keys {sorted(unknown)}; expected a subset of '
                     f'{sorted(DEFAULT_GRID)}')

    if options.plan_only:
        from fuzzydata.lineage.corpus import plan_corpus
        plan = plan_corpus(options.num_workflows, options.base_seed, grid=grid,
                           seed_tables=_seed_tables(options.seed_table_dir),
                           file_format=options.format,
                           distractor_pool=options.distractor_pool)
        json.dump(plan, sys.stdout, indent=2, default=str)
        sys.stdout.write('\n')
        return 0

    manifest = generate_corpus(
        output_dir=options.output_dir,
        num_workflows=options.num_workflows,
        base_seed=options.base_seed,
        grid=grid,
        seed_tables=_seed_tables(options.seed_table_dir),
        client=options.wf_client,
        base_shape=(options.columns, options.rows),
        file_format=options.format,
        distractor_pool=options.distractor_pool,
        workers=options.workers,
    )
    logger.info(f"Wrote {manifest['num_generated']} workflows "
                f"({manifest['num_failed']} failed) to {options.output_dir}/manifest.json")
    return 1 if manifest['num_failed'] else 0


if __name__ == '__main__':
    sys.exit(main())
