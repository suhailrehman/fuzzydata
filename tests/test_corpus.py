"""A8 acceptance: parquet round-trip, corpus determinism, distractors, idiom policy."""
import json
import logging

import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.core.workflow import detect_artifact_format
from fuzzydata.lineage.annotations import op_category
from fuzzydata.lineage.corpus import (derive_seed, expand_grid, generate_corpus, plan_corpus)
from fuzzydata.lineage.idioms import IDIOM_NAMES, IDIOMS, IdiomState

logger = logging.getLogger(__name__)

SMALL = dict(num_versions=[6], matfreq=[1], topology=['bfactor'],
             operator_policy=['schema_constrained'], bfactor=[1.0])


# --------------------------------------------------------------- serialization format

@pytest.mark.parametrize('file_format', ['csv', 'parquet'])
def test_generate_and_replay_in_both_formats(tmp_path, file_format):
    out = tmp_path / file_format
    wf = generate_workflow(DataFrameWorkflow, name='fmt', num_versions=8,
                           base_shape=(10, 300), out_directory=str(out), matfreq=1,
                           seed=55, file_format=file_format, validate='off')
    # Artifact filenames must match the format. These were hardcoded to .csv at three
    # creation sites while serialization used file_format, so they used to desynchronise.
    written = sorted(p.name for p in (out / 'artifacts').iterdir())
    assert written, 'no artifacts written'
    assert all(name.endswith(f'.{file_format}') for name in written), written
    assert len(written) == len(wf.artifact_dict)

    replayed = DataFrameWorkflow.load_workflow(str(out), str(tmp_path / f'replay_{file_format}'),
                                               replay=True)
    assert replayed is not None, 'replay returned nothing'
    assert set(replayed.artifact_dict) == set(wf.artifact_dict)


def test_detect_artifact_format(tmp_path):
    (tmp_path / 'a').mkdir()
    (tmp_path / 'a' / 'x.parquet').touch()
    assert detect_artifact_format(str(tmp_path / 'a')) == 'parquet'
    (tmp_path / 'b').mkdir()
    (tmp_path / 'b' / 'x.csv').touch()
    assert detect_artifact_format(str(tmp_path / 'b')) == 'csv'
    (tmp_path / 'c').mkdir()
    assert detect_artifact_format(str(tmp_path / 'c')) == 'csv'


def test_parquet_preserves_dtypes(tmp_path):
    """The reason to prefer parquet: csv round-trips everything through text, so dtypes
    survive only by inference."""
    wf = generate_workflow(DataFrameWorkflow, name='dt', num_versions=5,
                           base_shape=(10, 200), out_directory=str(tmp_path), matfreq=1,
                           seed=9, file_format='parquet', validate='off')
    label = wf.artifact_list[0]
    original = wf.artifact_dict[label].to_df()
    reread = pd.read_parquet(f'{tmp_path}/artifacts/{label}.parquet')
    assert list(original.dtypes) == list(reread.dtypes)


def test_no_mixed_type_object_columns(tmp_path):
    """A column whose values are part int and part str cannot be written to parquet at all.
    label_encode used to change a column's values to integer codes without updating the
    schema map, so a later fill faked a same-provider string into the now-integer column."""
    for seed in (300, 301, 302):
        wf = generate_workflow(DataFrameWorkflow, name=f'mx{seed}', num_versions=10,
                               base_shape=(8, 200), out_directory=str(tmp_path / str(seed)),
                               matfreq=2, seed=seed, file_format='parquet', validate='off')
        for label, artifact in wf.artifact_dict.items():
            df = artifact.to_df()
            for column in df.columns:
                if df[column].dtype == object:
                    kinds = {type(v).__name__ for v in df[column].dropna()}
                    assert len(kinds) <= 1, f'{label}.{column} is mixed: {kinds}'


# --------------------------------------------------------------- corpus planning

def test_derive_seed_is_stable_and_independent_of_corpus_size():
    assert derive_seed(7, 0) == derive_seed(7, 0)
    assert derive_seed(7, 0) != derive_seed(7, 1)
    assert derive_seed(7, 3) != derive_seed(8, 3)


def test_plan_is_a_pure_function():
    assert plan_corpus(5, 11, grid=SMALL) == plan_corpus(5, 11, grid=SMALL)


def test_expand_grid_is_ordered_and_complete():
    grid = {'matfreq': [1, 2], 'topology': ['star', 'chain']}
    combos = expand_grid(grid)
    assert len(combos) == len(expand_grid({**grid})) 
    assert {c['matfreq'] for c in combos} == {1, 2}
    assert {c['topology'] for c in combos} == {'star', 'chain'}
    assert combos == expand_grid(grid), 'expansion order must be deterministic'


def test_distractors_are_single_artifact_workflows(tmp_path):
    manifest = generate_corpus(str(tmp_path), num_workflows=2, base_seed=5, grid=SMALL,
                               base_shape=(8, 120), file_format='csv',
                               distractor_pool=2, workers=1)
    rows = {r['workflow_id']: r for r in manifest['workflows']}
    distractors = [r for r in rows.values() if r['kind'] == 'distractor']
    assert len(distractors) == 2
    for row in distractors:
        assert row['status'] == 'ok', row
        # A negative must have no lineage relationship to anything.
        assert row['num_artifacts'] == 1
        assert row['num_edges'] == 0


# --------------------------------------------------------------- corpus determinism

def test_corpus_determinism_across_worker_counts(tmp_path):
    """Same base seed and grid -> identical manifest, regardless of parallelism. Worker
    count must not leak into the output through seeds or through row ordering."""
    def run(name, workers):
        manifest = generate_corpus(str(tmp_path / name), num_workflows=4, base_seed=42,
                                   grid=SMALL, base_shape=(8, 120), file_format='csv',
                                   distractor_pool=1, workers=workers)
        for row in manifest['workflows']:
            row.pop('output_path')      # absolute paths differ by construction
        return manifest

    assert run('serial', 1) == run('parallel', 3)


def test_manifest_rows_are_sorted(tmp_path):
    manifest = generate_corpus(str(tmp_path), num_workflows=4, base_seed=1, grid=SMALL,
                               base_shape=(8, 120), file_format='csv', workers=2)
    ids = [r['workflow_id'] for r in manifest['workflows']]
    assert ids == sorted(ids)


def test_manifest_records_what_a_consumer_needs(tmp_path):
    manifest = generate_corpus(str(tmp_path), num_workflows=2, base_seed=3, grid=SMALL,
                               base_shape=(8, 120), file_format='csv', workers=1)
    with open(tmp_path / 'manifest.json') as infile:
        on_disk = json.load(infile)
    assert on_disk['base_seed'] == 3
    for row in manifest['workflows']:
        assert row['status'] == 'ok', row
        for field in ('workflow_id', 'seed', 'output_path', 'num_artifacts',
                      'operator_histogram', 'category_histogram', 'validity',
                      'topology', 'operator_policy', 'matfreq'):
            assert field in row, f'{field} missing from manifest row'


# --------------------------------------------------------------- idiom policy

def test_idiom_stages_only_reference_real_operators():
    from tests.test_operators import ALL_OPERATOR_NAMES
    referenced = {op for stages in IDIOMS.values() for stage in stages for op in stage}
    unknown = referenced - set(ALL_OPERATOR_NAMES)
    assert not unknown, f'idioms reference nonexistent operators: {unknown}'


def test_idiom_state_advances_and_falls_through():
    state = IdiomState('bi_rollup', adherence=1.0)
    assert state.stage == 0
    state.advance('project')
    assert state.stage == 1
    # An op outside the current stage must not advance it.
    state.advance('pivot')
    assert state.stage == 1


def test_idiom_never_blocks_generation():
    """When nothing the schema allows matches the idiom's stage, selection must fall through
    to a uniform choice rather than stall."""
    import numpy as np
    state = IdiomState('ml_prep', adherence=1.0)
    choices = [{'op': 'groupby', 'args': {}}]     # matches no ml_prep stage
    chosen = state.select(choices, np.random.default_rng(0))
    assert chosen['op'] == 'groupby'


@pytest.mark.parametrize('idiom', IDIOM_NAMES)
def test_every_idiom_generates(tmp_path, idiom):
    wf = generate_workflow(DataFrameWorkflow, name=f'id_{idiom}', num_versions=8,
                           base_shape=(10, 300), out_directory=str(tmp_path / idiom),
                           matfreq=2, seed=21, operator_policy='idiom', idiom=idiom,
                           validate='off')
    assert wf.graph.number_of_nodes() == 8
    assert wf.metadata['idiom'] == idiom
    assert wf.metadata['operator_policy'] == 'idiom'


def test_idiom_policy_shifts_the_category_distribution(tmp_path):
    """The point of the policy: within-workflow edge correlation, visible as a measurably
    different category mix from the schema-constrained default."""
    def categories(policy, idiom=None):
        counts = {}
        for seed in range(200, 210):
            wf = generate_workflow(DataFrameWorkflow,
                                   name=f'{policy}{idiom}{seed}', num_versions=10,
                                   base_shape=(10, 300),
                                   out_directory=str(tmp_path / f'{policy}{idiom}{seed}'),
                                   matfreq=2, seed=seed, operator_policy=policy,
                                   idiom=idiom, validate='off')
            for operation in wf.operation_list:
                for entry in operation['op_list']:
                    category = op_category(entry['op'])
                    counts[category] = counts.get(category, 0) + 1
        total = sum(counts.values())
        return {k: v / total for k, v in counts.items()}

    default = categories('schema_constrained')
    rollup = categories('idiom', 'bi_rollup')

    # bi_rollup steers toward aggregation; the default almost never reaches it.
    assert rollup.get('aggregation', 0) > default.get('aggregation', 0) * 2, (
        f'idiom did not shift the distribution: default={default} rollup={rollup}')


def test_unknown_policy_and_grid_keys_are_rejected(tmp_path):
    with pytest.raises(ValueError, match='operator_policy'):
        generate_workflow(DataFrameWorkflow, name='bad', num_versions=3,
                          base_shape=(8, 100), out_directory=str(tmp_path),
                          operator_policy='nonsense')
