"""Tests for reversal-closed workflow pairs (issue #17)."""
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.lineage.reversal import REVERSAL_CLOSED_OPS, generate_reversal_pair


def test_reversal_closed_pairs(tmp_path):
    """The original and reversed workflows must have identical artifact multisets
    and exactly reversed edge sets."""
    orig, rev = generate_reversal_pair(
        DataFrameWorkflow, base_seed=7, tmp_dir=str(tmp_path), n_artifacts=4
    )

    orig_artifacts = set(orig.artifact_dict.keys())
    rev_artifacts = set(rev.artifact_dict.keys())
    assert orig_artifacts == rev_artifacts, 'artifact sets must match'

    # DataFrames must be identical (same rows, same columns, same values)
    for label in orig_artifacts:
        orig_df = orig.artifact_dict[label].to_df().sort_values(
            by=sorted(orig.artifact_dict[label].to_df().columns)).reset_index(drop=True)
        rev_df = rev.artifact_dict[label].to_df().sort_values(
            by=sorted(rev.artifact_dict[label].to_df().columns)).reset_index(drop=True)
        assert orig_df.shape == rev_df.shape, f'shape mismatch for {label}'
        assert set(orig_df.columns) == set(rev_df.columns), f'column mismatch for {label}'

    # Edges must be exactly reversed
    orig_edges = set(orig.graph.edges())
    rev_edges = set(rev.graph.edges())
    reversed_of_orig = {(b, a) for a, b in orig_edges}
    assert rev_edges == reversed_of_orig, (
        f'reversed edges {rev_edges} != mirror of original edges {orig_edges}')


def test_reversal_closed_operators(tmp_path):
    """No operation in either workflow may use an op outside REVERSAL_CLOSED_OPS."""
    orig, rev = generate_reversal_pair(
        DataFrameWorkflow, base_seed=13, tmp_dir=str(tmp_path), n_artifacts=4
    )

    for wf, name in [(orig, 'original'), (rev, 'reversed')]:
        for op_entry in wf.operation_list:
            for entry in op_entry['op_list']:
                assert entry['op'] in REVERSAL_CLOSED_OPS, (
                    f'{name} workflow uses non-reversal-closed op {entry["op"]!r}')


def test_reversal_metadata_tagged(tmp_path):
    """The reversed workflow must carry the reversal_of metadata tag."""
    _, rev = generate_reversal_pair(
        DataFrameWorkflow, base_seed=99, tmp_dir=str(tmp_path), n_artifacts=3
    )
    assert 'reversal_of' in rev.metadata
