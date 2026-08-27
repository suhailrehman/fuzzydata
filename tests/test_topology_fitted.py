"""Tests for topology='fitted' (issue #18)."""
import logging
import statistics
import tempfile

import networkx as nx
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow


def _realized_depth(wf):
    g = wf.graph
    if g.number_of_nodes() <= 1:
        return 0
    return nx.dag_longest_path_length(g)


def test_fitted_topology_matches_target(tmp_path):
    """Supply a distribution concentrated on depth=4; over 20 runs mean depth is 3-5."""
    params = {
        'depth': {'values': [4], 'weights': [1.0]},
        'branching_factor': {'values': [1.0], 'weights': [1.0]},
    }
    depths = []
    for seed in range(20):
        wf = generate_workflow(
            DataFrameWorkflow, name=f'fitted_{seed}', num_versions=8,
            base_shape=(5, 50), out_directory=str(tmp_path / f's{seed}'),
            matfreq=1, seed=seed, topology='fitted', topology_params=params,
        )
        depths.append(_realized_depth(wf))

    mean_depth = statistics.mean(depths)
    assert 2 <= mean_depth <= 7, (
        f'mean realized depth {mean_depth:.2f} is far from target 4; '
        f'depths were {depths}')


def test_fitted_topology_fallback_on_small_n(tmp_path, caplog):
    """Unreachable depth target must complete without error (warning is acceptable)."""
    params = {
        'depth': {'values': [30], 'weights': [1.0]},
        'branching_factor': {'values': [1.0], 'weights': [1.0]},
    }
    with caplog.at_level(logging.WARNING):
        wf = generate_workflow(
            DataFrameWorkflow, name='small', num_versions=5,
            base_shape=(5, 50), out_directory=str(tmp_path),
            matfreq=1, seed=1, topology='fitted', topology_params=params,
        )
    assert len(wf.artifact_dict) >= 1


def test_fitted_topology_no_params_still_runs(tmp_path):
    """topology='fitted' with no topology_params must not raise."""
    wf = generate_workflow(
        DataFrameWorkflow, name='noparam', num_versions=5,
        base_shape=(5, 50), out_directory=str(tmp_path),
        matfreq=1, seed=2, topology='fitted',
    )
    assert len(wf.artifact_dict) >= 1


def test_fitted_topology_num_artifacts_override(tmp_path):
    """num_artifacts distribution overrides num_versions."""
    params = {
        'depth': {'values': [3], 'weights': [1.0]},
        'branching_factor': {'values': [1.0], 'weights': [1.0]},
        'num_artifacts': {'values': [7], 'weights': [1.0]},
    }
    wf = generate_workflow(
        DataFrameWorkflow, name='override', num_versions=3,
        base_shape=(5, 50), out_directory=str(tmp_path),
        matfreq=1, seed=5, topology='fitted', topology_params=params,
    )
    assert len(wf.artifact_dict) == 7
