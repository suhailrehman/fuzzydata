"""A5 acceptance: each named topology produces its characteristic shape.

Assertions are exact, not approximate: chain/star/balanced select their parent
deterministically, so there is nothing to be approximate about.

merge is excluded throughout. It synthesises an extra right-hand artifact and attaches it as
a second parent, which perturbs both out-degree and depth and would make the shape
assertions meaningless.
"""
import logging

import networkx as nx
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.core.workflow import Workflow

logger = logging.getLogger(__name__)

SEED = 7
N = 8
NO_MERGE = ['merge']


def _wf(tmp_path, topology, n=N, **kwargs):
    return generate_workflow(DataFrameWorkflow, name=f'topo_{topology}', num_versions=n,
                             base_shape=(8, 200), out_directory=str(tmp_path),
                             matfreq=1, seed=SEED, topology=topology,
                             exclude_ops=NO_MERGE, **kwargs)


def _shape(graph):
    roots = [n for n, d in graph.in_degree() if d == 0]
    depths = nx.shortest_path_length(graph, roots[0])
    return {
        'n': graph.number_of_nodes(),
        'max_out_degree': max(d for _, d in graph.out_degree()),
        'depth': max(depths.values()),
        'leaf_fraction': sum(1 for n, d in graph.out_degree() if d == 0) / graph.number_of_nodes(),
        'mean_branching': (graph.number_of_edges()
                           / max(1, sum(1 for _, d in graph.out_degree() if d > 0))),
    }


def test_star_is_a_star(tmp_path):
    """Every artifact hangs off the root: out-degree exactly n-1, depth exactly 1."""
    wf = _wf(tmp_path, 'star')
    shape = _shape(wf.graph)
    assert shape['max_out_degree'] == shape['n'] - 1
    assert shape['depth'] == 1


def test_chain_is_a_chain(tmp_path):
    """Each artifact derives from the previous: depth exactly n-1, out-degree exactly 1."""
    wf = _wf(tmp_path, 'chain')
    shape = _shape(wf.graph)
    assert shape['depth'] == shape['n'] - 1
    assert shape['max_out_degree'] == 1


def test_balanced_bounds_out_degree(tmp_path):
    """Breadth-first filling keeps out-degree even, so it must be strictly between the
    chain and star extremes."""
    wf = _wf(tmp_path, 'balanced', n=15)
    shape = _shape(wf.graph)
    assert 1 < shape['max_out_degree'] < shape['n'] - 1
    assert 1 < shape['depth'] < shape['n'] - 1


@pytest.mark.parametrize('topology', Workflow.TOPOLOGIES)
def test_every_topology_generates_a_dag(tmp_path, topology):
    wf = _wf(tmp_path, topology)
    assert nx.is_directed_acyclic_graph(wf.graph)
    assert wf.graph.number_of_nodes() == N


@pytest.mark.parametrize('bfactor', [0.01, 0.1, 1.0, 10.0, 100.0])
def test_bfactor_extremes_do_not_crash(tmp_path, bfactor):
    """bfactor=100 previously produced nan probabilities: the old leading constant
    bfactor/(exp(bfactor*size)-1) overflowed to 0. The README advertised exactly that value."""
    wf = generate_workflow(DataFrameWorkflow, name=f'bf_{bfactor}', num_versions=N,
                           base_shape=(8, 200), out_directory=str(tmp_path),
                           matfreq=1, seed=SEED, topology='bfactor', bfactor=bfactor,
                           exclude_ops=NO_MERGE)
    assert wf.graph.number_of_nodes() == N


def test_high_bfactor_is_chain_like_and_low_is_branchy(tmp_path):
    """The direction the docs had backwards through 0.0.11."""
    high = _shape(_wf(tmp_path / 'hi', 'bfactor', bfactor=100.0).graph)
    low = _shape(_wf(tmp_path / 'lo', 'bfactor', bfactor=0.01).graph)
    assert high['depth'] > low['depth']
    assert high['max_out_degree'] <= low['max_out_degree']
