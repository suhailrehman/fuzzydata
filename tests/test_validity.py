"""S9 acceptance: generated artifacts are non-degenerate, and degeneracy is detected."""
import logging

import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.lineage.validity import (DegenerateArtifactError, MIN_COLUMNS, MIN_ROWS,
                                        check_artifact, describe_artifact,
                                        validate_workflow, workflow_validity_summary)

logger = logging.getLogger(__name__)


def test_check_artifact_accepts_a_healthy_frame():
    df = pd.DataFrame({'a': range(50), 'b': [i % 7 for i in range(50)]})
    assert check_artifact(df) == []


@pytest.mark.parametrize('df, expect', [
    (pd.DataFrame({'a': [], 'b': []}), 'rows'),
    (pd.DataFrame({'a': range(2), 'b': range(2)}), 'rows'),
    (pd.DataFrame({'a': range(50)}), 'columns'),
    (pd.DataFrame({'a': [1] * 50, 'b': [2] * 50}), 'constant'),
    (pd.DataFrame({'a': range(50), 'b': [None] * 50}), 'all-null'),
])
def test_check_artifact_detects_degeneracy(df, expect):
    problems = check_artifact(df, 'x')
    assert problems, f'expected a problem for {expect}'
    assert any(expect in p for p in problems), problems


def test_describe_artifact_summary():
    df = pd.DataFrame({'a': range(10), 'const': [1] * 10, 'nulls': [None] * 10})
    summary = describe_artifact(df)
    assert summary['rows'] == 10
    assert summary['columns'] == 3
    assert 'const' in summary['constant_columns']
    assert 'nulls' in summary['all_null_columns']


def _wf(tmp_path, seed, matfreq=1, **kwargs):
    return generate_workflow(DataFrameWorkflow, name=f'val{seed}', num_versions=12,
                             base_shape=(10, 300), out_directory=str(tmp_path),
                             matfreq=matfreq, seed=seed, validate='off', **kwargs)


@pytest.mark.parametrize('seed', [101, 102, 106, 110, 112])
def test_generated_workflows_are_mostly_non_degenerate(tmp_path, seed):
    """The generator pre-empts the main degeneracy causes: groupby below the row floor,
    over-wide or too-short pivots, single-column projects, and over-aggressive samples.
    With matfreq=1 the guards see the true source artifact, so nothing should slip through."""
    wf = _wf(tmp_path, seed)
    problems = validate_workflow(wf)
    assert not problems, f'degenerate artifacts at matfreq=1: {problems}'


def test_validity_summary_shape(tmp_path):
    summary = workflow_validity_summary(_wf(tmp_path, 101))
    assert summary['artifacts'] == 12
    assert summary['min_rows'] >= MIN_ROWS
    assert summary['min_columns'] >= MIN_COLUMNS
    assert summary['degenerate_artifacts'] == 0


def test_strict_mode_raises(tmp_path):
    """A workflow with a deliberately degenerate artifact must fail loudly under strict."""
    wf = _wf(tmp_path, 101)
    # Replace one artifact's contents with something degenerate.
    victim = wf.artifact_dict[wf.artifact_list[-1]]
    victim.from_df(pd.DataFrame({'only': [1, 2]}))
    with pytest.raises(DegenerateArtifactError, match='degenerate'):
        validate_workflow(wf, strict=True)


def test_validate_option_is_honoured(tmp_path):
    """validate='off' must not raise even when something is wrong; the default warns."""
    wf = generate_workflow(DataFrameWorkflow, name='v', num_versions=6,
                           base_shape=(10, 200), out_directory=str(tmp_path),
                           matfreq=1, seed=101, validate='warn')
    assert wf.graph.number_of_nodes() == 6
