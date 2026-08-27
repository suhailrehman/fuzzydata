"""A1' acceptance: pin generation behaviour so later work items cannot change it silently.

Two kinds of assertion here, and the distinction matters:

  * EXACT baselines, keyed on a fixed seed. Only meaningful because A2 made generation
    deterministic -- pinning exact counts before that would have been pinning noise. When a
    work item legitimately changes them, regenerate the baseline and let the diff be
    reviewed. Regenerate with:
        python -m tests.test_characterization --update-baseline

  * SEED-INDEPENDENT invariants, which must hold for every seed. These are the real safety
    net: they cannot go stale and they do not need updating when the operator mix shifts.
"""
import collections
import json
import logging
import os
import sys
import tempfile

import networkx as nx
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.lineage.annotations import OP_CATEGORY
from fuzzydata.lineage.validity import check_artifact

logger = logging.getLogger(__name__)

BASELINE_PATH = os.path.join(os.path.dirname(__file__), 'fixtures',
                             'characterization_baseline.json')


def _load_baseline():
    with open(BASELINE_PATH) as infile:
        return json.load(infile)


def _generate(config, out_directory):
    return generate_workflow(DataFrameWorkflow, name='char',
                             num_versions=config['num_versions'],
                             base_shape=tuple(config['base_shape']),
                             out_directory=out_directory,
                             matfreq=config['matfreq'], seed=config['seed'],
                             topology=config['topology'], validate='off')


def _summarize(wf):
    histogram = collections.Counter(entry['op'] for operation in wf.operation_list
                                    for entry in operation['op_list'])
    roots = [n for n, d in wf.graph.in_degree() if d == 0]
    depths = nx.shortest_path_length(wf.graph, roots[0])
    return {
        'num_artifacts': len(wf.artifact_dict),
        'num_edges': wf.graph.number_of_edges(),
        'num_operations': len(wf.operation_list),
        'operator_histogram': dict(sorted(histogram.items())),
        'max_depth': max(depths.values()),
        'max_out_degree': max(d for _, d in wf.graph.out_degree()),
        'artifact_labels': list(wf.artifact_list),
    }


# ------------------------------------------------------------------ exact baselines

@pytest.mark.parametrize('entry', _load_baseline(),
                         ids=lambda e: f"mf{e['config']['matfreq']}-{e['config']['topology']}")
def test_generation_matches_baseline(tmp_path, entry):
    actual = _summarize(_generate(entry['config'], str(tmp_path)))
    expected = {k: v for k, v in entry.items() if k != 'config'}
    assert actual == expected, (
        'Generation behaviour changed. If this is intentional, regenerate the baseline with '
        '`python -m tests.test_characterization --update-baseline` and review the diff.')


def test_baseline_is_reproducible_within_a_run(tmp_path):
    """The same config twice in one process must agree -- catches leaked global RNG state,
    which is exactly what A2 removed."""
    config = _load_baseline()[0]['config']
    assert (_summarize(_generate(config, str(tmp_path / 'a')))
            == _summarize(_generate(config, str(tmp_path / 'b'))))


# ------------------------------------------------------- seed-independent invariants

SEEDS = [1, 17, 404, 90210]


@pytest.mark.parametrize('seed', SEEDS)
@pytest.mark.parametrize('matfreq', [1, 3])
def test_invariants_hold_for_any_seed(tmp_path, seed, matfreq):
    wf = generate_workflow(DataFrameWorkflow, name='inv', num_versions=10,
                           base_shape=(10, 300), out_directory=str(tmp_path / f'{seed}{matfreq}'),
                           matfreq=matfreq, seed=seed, validate='off')

    assert nx.is_directed_acyclic_graph(wf.graph), 'lineage graph must be a DAG'
    assert len(wf.artifact_dict) == wf.graph.number_of_nodes()
    assert len(wf.artifact_list) == len(set(wf.artifact_list)), 'duplicate artifact labels'

    # Roots are NOT unique: merge synthesises its right-hand table via
    # generate_pkfk_join_table() and adds it as a parentless artifact, so each merge
    # contributes one extra root. Assert that exact relationship rather than "one root",
    # which is the stronger and actually-true invariant.
    roots = [n for n, d in wf.graph.in_degree() if d == 0]
    merges = sum(1 for operation in wf.operation_list
                 for entry in operation['op_list'] if entry['op'] == 'merge')
    assert len(roots) == 1 + merges, (
        f'expected {1 + merges} roots (base artifact + one per merge), got {roots}')
    assert wf.artifact_list[0] in roots, 'the base artifact must be a root'

    for label, artifact in wf.artifact_dict.items():
        assert artifact.schema_map, f'{label} has an empty schema map'
        columns = set(artifact.to_df().columns)
        assert set(artifact.schema_map) == columns, (
            f'{label}: schema map disagrees with materialized columns')

    for operation in wf.operation_list:
        assert operation['new_label'] in wf.artifact_dict
        assert operation['op_list'], 'an operation recorded no transformations'
        for source in operation['sources']:
            assert source in wf.artifact_dict
        annotation = operation['annotation']
        assert annotation['composition_depth'] == len(operation['op_list'])
        assert not annotation['unrecognized_ops']

    used = {entry['op'] for operation in wf.operation_list for entry in operation['op_list']}
    assert used <= set(OP_CATEGORY), f'uncategorised operators: {used - set(OP_CATEGORY)}'


@pytest.mark.parametrize('seed', SEEDS)
def test_no_degenerate_artifacts_at_matfreq_one(tmp_path, seed):
    """At matfreq=1 the generator's pre-conditions see the true source artifact, so the
    validity guards should hold exactly. (At higher matfreq they work from a stale view --
    see fuzzydata.lineage.validity.)"""
    wf = generate_workflow(DataFrameWorkflow, name='deg', num_versions=10,
                           base_shape=(10, 300), out_directory=str(tmp_path / str(seed)),
                           matfreq=1, seed=seed, validate='off')
    problems = {label: check_artifact(a.to_df(), label)
                for label, a in wf.artifact_dict.items()}
    assert not any(problems.values()), {k: v for k, v in problems.items() if v}


@pytest.mark.parametrize('seed', SEEDS)
def test_all_sidecar_files_are_written(tmp_path, seed):
    out = tmp_path / str(seed)
    wf = generate_workflow(DataFrameWorkflow, name='side', num_versions=8,
                           base_shape=(10, 300), out_directory=str(out), matfreq=1,
                           seed=seed, validate='off')
    for suffix in ('_operations.json', '_gt_graph.csv', '_schema_map.json',
                   '_equivalence_classes.json', '_code.py'):
        assert (out / f'{wf.name}{suffix}').exists(), f'missing sidecar {suffix}'
    assert len(list((out / 'artifacts').iterdir())) == len(wf.artifact_dict)


def _update_baseline():
    """Regenerate the committed baseline. Deliberately a manual step."""
    baseline = _load_baseline()
    for entry in baseline:
        with tempfile.TemporaryDirectory() as tmp:
            entry.update(_summarize(_generate(entry['config'], tmp)))
    with open(BASELINE_PATH, 'w') as outfile:
        json.dump(baseline, outfile, indent=2)
    print(f'Baseline rewritten: {BASELINE_PATH}')


if __name__ == '__main__':
    if '--update-baseline' in sys.argv:
        logging.disable(logging.CRITICAL)
        _update_baseline()
    else:
        print(__doc__)
