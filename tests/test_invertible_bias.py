"""Acceptance tests for the invertible_bias operator-weighting knob (issue #15)."""
import collections
import tempfile

import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.lineage.equivalence import compute_equivalence_classes

SEED = 42
BASE_SHAPE = (10, 300)
NUM_VERSIONS = 20


def _histogram(wf):
    hist = collections.Counter()
    for op in wf.operation_list:
        for entry in op['op_list']:
            hist[entry['op']] += 1
    return dict(hist)


def test_default_bias_unchanged(tmp_path):
    """invertible_bias=0 must produce the same operator frequencies as the unbiased default."""
    def run(path):
        return _histogram(generate_workflow(
            DataFrameWorkflow, name='base', num_versions=NUM_VERSIONS,
            base_shape=BASE_SHAPE, out_directory=str(path),
            matfreq=1, seed=SEED, invertible_bias=0.0,
        ))

    h1 = run(tmp_path / 'a')
    h2 = run(tmp_path / 'b')
    assert h1 == h2, 'same seed and bias=0 must give identical histograms'

    # Also check against the true unbiased default (no invertible_bias kwarg at all)
    h_default = _histogram(generate_workflow(
        DataFrameWorkflow, name='noarg', num_versions=NUM_VERSIONS,
        base_shape=BASE_SHAPE, out_directory=str(tmp_path / 'c'),
        matfreq=1, seed=SEED,
    ))
    assert h1 == h_default, 'invertible_bias=0.0 must be identical to omitting the argument'


def test_bias_raises_class_size(tmp_path):
    """High invertible_bias must increase the share of artifacts in non-trivial classes."""
    def non_trivial_share(bias, subdir):
        wf = generate_workflow(
            DataFrameWorkflow, name='biased', num_versions=NUM_VERSIONS,
            base_shape=BASE_SHAPE, out_directory=str(tmp_path / subdir),
            matfreq=1, seed=SEED, invertible_bias=bias,
        )
        classes = compute_equivalence_classes(wf)
        # classes is {artifact_label: class_id}; count members per class_id
        size_per_class = collections.Counter(classes.values())
        total = len(classes)
        # an artifact is "non-trivial" when its class contains more than one member
        non_trivial = sum(count for count in size_per_class.values() if count > 1)
        return non_trivial / total if total else 0.0

    baseline = non_trivial_share(0.0, 'bias0')
    high = non_trivial_share(5.0, 'bias5')

    assert high > baseline, (
        f'high bias ({high:.2%}) should exceed baseline ({baseline:.2%}) '
        'in non-trivial class share'
    )
    # At least some non-trivial classes must appear
    assert high > 0.0, 'expected at least one non-trivial equivalence class at bias=5.0'


def test_bias_is_recorded_in_metadata(tmp_path):
    """invertible_bias must appear in workflow metadata."""
    wf = generate_workflow(
        DataFrameWorkflow, name='meta', num_versions=5,
        base_shape=BASE_SHAPE, out_directory=str(tmp_path),
        matfreq=1, seed=SEED, invertible_bias=3.0,
    )
    assert wf.metadata.get('invertible_bias') == 3.0
