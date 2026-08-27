"""A2 acceptance: seeded generation is reproducible, and a serialized workflow replays.

Note the criterion is frame equality plus a content hash over sorted rows, NOT byte
comparison of the artifact files: parquet is not byte-reproducible across pyarrow versions,
and CSV float formatting is lossy. See docs/topology.md and the CHANGELOG for scope.
"""
import glob
import hashlib
import json
import logging

import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow

logger = logging.getLogger(__name__)

SEED = 20260827
BASE_SHAPE = (10, 300)
NUM_VERSIONS = 8


def _content_hash(df: pd.DataFrame) -> str:
    """Order-insensitive content digest: sort columns, then sort rows by their string form."""
    ordered = df.reindex(sorted(df.columns), axis=1)
    as_text = ordered.astype(str)
    rows = sorted('\x1f'.join(r) for r in as_text.itertuples(index=False, name=None))
    return hashlib.sha256('\x1e'.join(rows).encode()).hexdigest()


def _generate(tmp_path, seed, name='det', **kwargs):
    return generate_workflow(DataFrameWorkflow, name=name, num_versions=NUM_VERSIONS,
                             base_shape=BASE_SHAPE, out_directory=str(tmp_path),
                             matfreq=2, seed=seed, **kwargs)


def _fingerprint(wf):
    """Everything a corpus consumer would care about: graph, operations, and contents."""
    return {
        'artifacts': list(wf.artifact_list),
        'edges': sorted(wf.graph.edges()),
        'operations': [json.dumps(op, sort_keys=True, default=str) for op in wf.operation_list],
        'contents': {label: _content_hash(a.to_df()) for label, a in wf.artifact_dict.items()},
    }


def test_seed_reproducibility(tmp_path):
    """Same seed twice -> identical graph, operations and artifact contents."""
    first = _fingerprint(_generate(tmp_path / 'a', SEED))
    second = _fingerprint(_generate(tmp_path / 'b', SEED))

    assert first['artifacts'] == second['artifacts']
    assert first['edges'] == second['edges']
    assert first['operations'] == second['operations']
    assert first['contents'] == second['contents']


def test_different_seeds_differ(tmp_path):
    """Guards against the seed being silently ignored, which would make the test above
    pass trivially."""
    first = _fingerprint(_generate(tmp_path / 'a', SEED))
    second = _fingerprint(_generate(tmp_path / 'b', SEED + 1))
    assert (first['operations'], first['contents']) != (second['operations'], second['contents'])


def test_stochastic_ops_record_their_seed(tmp_path):
    """Every stochastic operation must persist the realized randomness, or replay cannot
    reproduce it. This is the concrete defect A2 exists to fix: sample previously emitted
    .sample(frac=...) with no random_state."""
    # Exclude every other operator so sample is guaranteed to be chosen -- otherwise this
    # test depends on which op the seed happens to pick. Derived from the operator list
    # rather than hardcoded, so adding an operator cannot silently defeat this test.
    from tests.test_operators import ALL_OPERATOR_NAMES
    wf = _generate(tmp_path, SEED,
                   exclude_ops=[op for op in ALL_OPERATOR_NAMES if op != 'sample'])
    samples = [o for op in wf.operation_list for o in op['op_list'] if o['op'] == 'sample']
    assert samples, 'expected at least one sample operation in the generated workflow'
    for op in samples:
        assert isinstance(op['args'].get('random_state'), int), op

    # ... and it must reach the emitted code, not just the record.
    code = glob.glob(f"{wf.out_dir}/*_code.py")
    assert code
    emitted = open(code[0]).read()
    assert 'random_state=' in emitted


def test_replay_reproduces_artifacts(tmp_path):
    """generate -> serialize -> replay -> same contents, including stochastic operators."""
    wf = _generate(tmp_path / 'orig', SEED)
    original = {label: _content_hash(a.to_df()) for label, a in wf.artifact_dict.items()}

    replayed = DataFrameWorkflow.load_workflow(str(tmp_path / 'orig'),
                                               str(tmp_path / 'replay'), replay=True)
    replayed_hashes = {label: _content_hash(a.to_df())
                       for label, a in replayed.artifact_dict.items()}

    assert set(replayed_hashes) == set(original), 'replay produced a different artifact set'
    mismatched = [k for k in original if original[k] != replayed_hashes[k]]
    assert not mismatched, f'replay diverged for: {mismatched}'
