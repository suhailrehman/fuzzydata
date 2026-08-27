# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.corpus
~~~~~~~~~~~~

Corpus-scale workflow generation.

Generates many workflows over a parameter grid, fans out across processes, and writes a
manifest describing what was produced.

Determinism is the design constraint. Every workflow's seed is derived from the base seed and
the workflow's index alone -- never from process id, wall-clock time, completion order, or
the number of workers -- so the same base seed and grid reproduce the same corpus regardless
of how it was parallelised. The manifest is sorted by workflow id for the same reason:
multiprocessing completion order varies, and an unsorted manifest would not be comparable
between runs.
"""

import importlib.metadata
import itertools
import json
import logging
import multiprocessing
import os
import platform
import sys
import traceback
from typing import Dict, List, Optional, Sequence

import numpy as np

from fuzzydata.lineage.validity import workflow_validity_summary

logger = logging.getLogger(__name__)


def _build_environment() -> Dict:
    """Snapshot of the generating environment for manifest reproducibility."""
    def _ver(pkg):
        try:
            return importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            return None

    return {
        'fuzzydata': _ver('fuzzydata'),
        'python': platform.python_version(),
        'platform': platform.platform(),
        'pandas': _ver('pandas'),
        'numpy': _ver('numpy'),
        'pyarrow': _ver('pyarrow'),
        'networkx': _ver('networkx'),
    }


#: Grid dimensions expanded by expand_grid(), in a fixed order so the expansion is stable.
GRID_KEYS = ('num_versions', 'matfreq', 'topology', 'operator_policy', 'bfactor')

DEFAULT_GRID = {
    'num_versions': [10],
    'matfreq': [1, 2],
    'topology': ['bfactor', 'star', 'chain'],
    'operator_policy': ['schema_constrained', 'idiom'],
    'bfactor': [1.0],
}


def derive_seed(base_seed: int, index: int) -> int:
    """Seed for workflow `index`, derived from `base_seed` alone.

    Uses SeedSequence with a spawn key rather than base_seed + index: consecutive integer
    seeds produce correlated streams, and spawn keys are designed for exactly this. Crucially
    the result does not depend on how many workflows there are or how many workers run, so a
    corpus is reproducible across worker counts.
    """
    sequence = np.random.SeedSequence(entropy=int(base_seed), spawn_key=(int(index),))
    return int(sequence.generate_state(1, dtype=np.uint32)[0])


def expand_grid(grid: Dict[str, Sequence] = None) -> List[Dict]:
    """Cartesian product of the parameter grid, in a deterministic order."""
    grid = {**DEFAULT_GRID, **(grid or {})}
    keys = [k for k in GRID_KEYS if k in grid]
    combos = itertools.product(*(grid[k] for k in keys))
    return [dict(zip(keys, combo)) for combo in combos]


def plan_corpus(num_workflows: int, base_seed: int, grid: Dict[str, Sequence] = None,
                seed_tables: Sequence[str] = None, file_format: str = 'parquet',
                distractor_pool: int = 0) -> List[Dict]:
    """Build the full list of workflow specs, without generating anything.

    Separated from execution so the plan can be inspected, sharded or diffed. The plan is a
    pure function of its arguments.

    :param num_workflows: how many lineage workflows to generate.
    :param base_seed: root seed; every workflow seed is derived from it.
    :param grid: parameter grid; see DEFAULT_GRID.
    :param seed_tables: optional real seed tables, assigned round-robin.
    :param file_format: 'csv' or 'parquet'.
    :param distractor_pool: number of single-artifact workflows to add as negatives. These
        exist so a corpus contains artifacts with no lineage relationship to the rest --
        without them every candidate pair is related and precision cannot be measured.
    :return: list of specs, each fully determined and independently generatable.
    """
    combos = expand_grid(grid)
    tables = list(seed_tables or [])
    specs = []

    for index in range(num_workflows):
        params = dict(combos[index % len(combos)])
        specs.append({
            'workflow_id': f'wf_{index:05d}',
            'index': index,
            'kind': 'lineage',
            'seed': derive_seed(base_seed, index),
            'file_format': file_format,
            'base_artifact': tables[index % len(tables)] if tables else None,
            **params,
        })

    for offset in range(distractor_pool):
        index = num_workflows + offset
        specs.append({
            'workflow_id': f'distractor_{offset:05d}',
            'index': index,
            'kind': 'distractor',
            'seed': derive_seed(base_seed, index),
            'file_format': file_format,
            'base_artifact': tables[index % len(tables)] if tables else None,
            # A distractor is a single artifact with no derivations: num_versions=1 means
            # generate_workflow produces the base artifact and stops.
            'num_versions': 1,
            'matfreq': 1,
            'topology': 'bfactor',
            'operator_policy': 'schema_constrained',
            'bfactor': 1.0,
        })

    return specs


def _generate_one(job: Dict) -> Dict:
    """Generate a single workflow. Module-level and self-contained so it can be pickled to a
    worker process. Returns a manifest row; never raises, so one bad workflow cannot abort
    the corpus."""
    from fuzzydata.clients import supported_workflows
    from fuzzydata.core.generator import generate_workflow
    from fuzzydata.lineage.annotations import op_category

    spec, output_dir, client = job['spec'], job['output_dir'], job['client']
    workflow_dir = os.path.join(output_dir, spec['workflow_id'])

    row = {k: spec[k] for k in spec}
    row['output_path'] = workflow_dir
    row['client'] = client

    try:
        workflow = generate_workflow(
            supported_workflows[client],
            name=spec['workflow_id'],
            num_versions=spec['num_versions'],
            base_shape=tuple(job['base_shape']),
            out_directory=workflow_dir,
            matfreq=spec['matfreq'],
            bfactor=spec['bfactor'],
            topology=spec['topology'],
            operator_policy=spec['operator_policy'],
            seed=spec['seed'],
            base_artifact=spec['base_artifact'],
            file_format=spec['file_format'],
            validate='off',   # summarised into the manifest instead of logged per workflow
        )
    except Exception as e:
        logger.error(f'{spec["workflow_id"]} failed: {e}')
        row.update({'status': 'error', 'error': f'{type(e).__name__}: {e}',
                    'traceback': traceback.format_exc(limit=3)})
        return row

    histogram = {}
    for operation in workflow.operation_list:
        for entry in operation['op_list']:
            histogram[entry['op']] = histogram.get(entry['op'], 0) + 1
    categories = {}
    for op, count in histogram.items():
        category = op_category(op)
        categories[category] = categories.get(category, 0) + count

    seed_descriptor = None
    if spec.get('base_artifact'):
        try:
            from fuzzydata.core.generator import load_seed_table
            from fuzzydata.lineage.profiler import describe_table
            seed_df = load_seed_table(spec['base_artifact'])
            seed_descriptor = describe_table(seed_df)
        except Exception as e:
            logger.warning(f'Could not describe seed table {spec["base_artifact"]}: {e}')

    row.update({
        'status': 'ok',
        'idiom': (workflow.metadata or {}).get('idiom'),
        'num_artifacts': len(workflow.artifact_dict),
        'num_edges': workflow.graph.number_of_edges(),
        'operator_histogram': dict(sorted(histogram.items())),
        'category_histogram': dict(sorted(categories.items())),
        'validity': workflow_validity_summary(workflow),
    })
    if seed_descriptor is not None:
        row['seed_descriptor'] = seed_descriptor
    return row


def generate_corpus(output_dir: str, num_workflows: int = 10, base_seed: int = 0,
                    grid: Dict[str, Sequence] = None, seed_tables: Sequence[str] = None,
                    client: str = 'pandas', base_shape=(10, 1000),
                    file_format: str = 'parquet', distractor_pool: int = 0,
                    workers: Optional[int] = None) -> Dict:
    """Generate a corpus and write `manifest.json`.

    :param output_dir: directory to write the corpus into.
    :param num_workflows: number of lineage workflows.
    :param base_seed: root seed for the whole corpus.
    :param grid: parameter grid; see DEFAULT_GRID.
    :param seed_tables: optional list of real seed table paths, assigned round-robin.
    :param client: workflow client name ('pandas' or 'sql').
    :param base_shape: (columns, rows) for generated base artifacts; ignored per workflow
        when that workflow has a seed table.
    :param file_format: 'csv' or 'parquet'.
    :param distractor_pool: number of single-artifact negatives to add.
    :param workers: process count; None uses cpu_count(). Has NO effect on the output.
    :return: the manifest dict.
    """
    os.makedirs(output_dir, exist_ok=True)
    specs = plan_corpus(num_workflows, base_seed, grid=grid, seed_tables=seed_tables,
                        file_format=file_format, distractor_pool=distractor_pool)
    jobs = [{'spec': spec, 'output_dir': output_dir, 'client': client,
             'base_shape': list(base_shape)} for spec in specs]

    workers = workers or min(multiprocessing.cpu_count(), len(jobs)) or 1
    logger.info(f'Generating {len(jobs)} workflows into {output_dir} using {workers} '
                f'worker(s), base_seed={base_seed}')

    if workers == 1:
        rows = [_generate_one(job) for job in jobs]
    else:
        with multiprocessing.Pool(processes=workers) as pool:
            rows = pool.map(_generate_one, jobs)

    # Sort by workflow_id: pool.map preserves input order, but imap/completion order does
    # not, and an unsorted manifest would make two runs incomparable. Sorting explicitly
    # makes the guarantee independent of which pool method is used.
    rows.sort(key=lambda r: r['workflow_id'])

    failed = [r for r in rows if r.get('status') != 'ok']
    manifest = {
        'base_seed': base_seed,
        'client': client,
        'environment': _build_environment(),
        'file_format': file_format,
        'num_workflows': num_workflows,
        'distractor_pool': distractor_pool,
        'base_shape': list(base_shape),
        'seed_tables': list(seed_tables or []),
        'grid': {**DEFAULT_GRID, **(grid or {})},
        'num_generated': len(rows),
        'num_failed': len(failed),
        'workflows': rows,
    }

    manifest_path = os.path.join(output_dir, 'manifest.json')
    with open(manifest_path, 'w') as outfile:
        json.dump(manifest, outfile, indent=2, sort_keys=True, default=str)

    degenerate = sum(r.get('validity', {}).get('degenerate_artifacts', 0) for r in rows)
    total = sum(r.get('num_artifacts', 0) for r in rows)
    logger.info(f'Corpus written to {manifest_path}: {len(rows)} workflows, '
                f'{total} artifacts, {len(failed)} failed, {degenerate} degenerate artifacts')
    if failed:
        logger.warning(f'Failed workflows: {[r["workflow_id"] for r in failed]}')

    return manifest
