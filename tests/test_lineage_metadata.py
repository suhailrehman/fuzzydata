"""A6 + A7 acceptance: per-edge provenance annotations, and equivalence classes."""
import json
import logging

import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameOperation, DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow
from fuzzydata.lineage.annotations import (AUGMENTING_OPS, CATEGORIES, OP_CATEGORY,
                                           STOCHASTIC_OPS, annotate_edge)
from fuzzydata.lineage.equivalence import compute_equivalence_classes

logger = logging.getLogger(__name__)

ANNOTATION_FIELDS = {'composition_depth', 'categories', 'stochastic', 'augmenting',
                     'invertible_on_input', 'sibling_group', 'ops', 'unrecognized_ops'}


def _wf(tmp_path, **kwargs):
    kwargs.setdefault('matfreq', 2)
    return generate_workflow(DataFrameWorkflow, name='meta', num_versions=12,
                             base_shape=(10, 400), out_directory=str(tmp_path),
                             seed=31, **kwargs)


# --------------------------------------------------------------------------- A6

def test_annotation_completeness(tmp_path):
    """Every ground-truth edge carries a record with all fields populated."""
    wf = _wf(tmp_path)
    assert wf.operation_list, 'expected at least one operation'
    for operation in wf.operation_list:
        annotation = operation.get('annotation')
        assert annotation is not None, f'{operation["new_label"]} has no annotation'
        assert set(annotation) == ANNOTATION_FIELDS
        assert annotation['composition_depth'] == len(operation['op_list'])
        assert annotation['composition_depth'] >= 1
        assert all(c in CATEGORIES for c in annotation['categories'])
        assert isinstance(annotation['stochastic'], bool)
        assert isinstance(annotation['augmenting'], bool)
        assert isinstance(annotation['invertible_on_input'], bool)
        assert not annotation['unrecognized_ops'], (
            f'uncategorised operators leaked into the corpus: '
            f'{annotation["unrecognized_ops"]}')


def test_every_generatable_op_has_a_category():
    """A new operator must not reach the corpus uncategorised."""
    from tests.test_operators import ALL_OPERATOR_NAMES
    missing = [op for op in ALL_OPERATOR_NAMES if op not in OP_CATEGORY]
    assert not missing, f'operators without a category: {missing}'


def test_composition_depth_is_realized_not_configured(tmp_path):
    """matfreq is an upper bound: a merge forces materialization and pivot is chain-final,
    so the recorded depth must be what actually happened."""
    wf = _wf(tmp_path, matfreq=3)
    depths = {op['annotation']['composition_depth'] for op in wf.operation_list}
    assert depths, 'no operations recorded'
    assert max(depths) <= 3
    assert min(depths) >= 1


def test_stochastic_and_augmenting_flags_track_the_ops(tmp_path):
    wf = _wf(tmp_path)
    for operation in wf.operation_list:
        names = {e['op'] for e in operation['op_list']}
        annotation = operation['annotation']
        assert annotation['stochastic'] == bool(names & STOCHASTIC_OPS)
        assert annotation['augmenting'] == bool(names & AUGMENTING_OPS)


def test_augmenting_is_a_category_not_also_a_boolean():
    """The original spec had `augmenting` both as a category value and as a separate
    boolean, which permitted contradictory records. The boolean is derived from the
    category set, so they cannot disagree."""
    assert 'augmenting' in CATEGORIES
    for op in AUGMENTING_OPS:
        assert OP_CATEGORY[op] == 'augmenting'


# --------------------------------------------------------------------------- A7

def _artifact(tmp_path, df, name='src'):
    return DataFrameArtifact(name, filename=str(tmp_path / f'{name}.csv'), from_df=df,
                             schema_map={c: '__profiled_numeric' if
                                         pd.api.types.is_numeric_dtype(df[c])
                                         else '__profiled_string' for c in df.columns})


@pytest.mark.parametrize('op, args, expected', [
    ('rename', {'column_map': {'val': 'val2'}}, True),
    ('apply', {'numeric_col': 'val', 'a': 3, 'b': 1}, True),
    ('apply', {'numeric_col': 'val', 'a': 0, 'b': 1}, False),
    ('project', {'output_cols': ['val']}, False),
    ('groupby', {'group_columns': ['grp'], 'agg_columns': ['val'],
                 'agg_function': 'sum'}, False),
    ('sample', {'frac': 0.5, 'random_state': 1}, False),
    ('dropna', {'subset': None}, False),
    ('dedupe', {'subset': None}, False),
    ('normalize', {'column': 'val'}, False),
    ('label_encode', {'column': 'grp'}, False),
])
def test_invertibility_rules(tmp_path, op, args, expected):
    df = pd.DataFrame({'val': [1, 2, 3, 4] * 5, 'grp': ['a', 'b'] * 10})
    artifact = _artifact(tmp_path, df)
    operation = DataFrameOperation(sources=[artifact])
    operation.chain_operation(op, args)
    assert operation.is_invertible_on(artifact) is expected


def test_astype_invertible_only_when_widening(tmp_path):
    df = pd.DataFrame({'i': [1, 2, 3], 'f': [1.5, 2.5, 3.5]})
    artifact = _artifact(tmp_path, df)

    widening = DataFrameOperation(sources=[artifact])
    widening.chain_operation('astype', {'column': 'i', 'dtype': 'float64'})
    assert widening.is_invertible_on(artifact) is True

    narrowing = DataFrameOperation(sources=[artifact])
    narrowing.chain_operation('astype', {'column': 'f', 'dtype': 'int64'})
    assert narrowing.is_invertible_on(artifact) is False


def test_fill_invertibility_is_computed_from_the_data(tmp_path):
    """Invertible iff the replacement value was not already present -- if it was, the
    output has two indistinguishable populations."""
    df = pd.DataFrame({'c': ['x', 'y', 'z'] * 5})
    artifact = _artifact(tmp_path, df)

    fresh = DataFrameOperation(sources=[artifact])
    fresh.chain_operation('fill', {'col_name': 'c', 'old_value': 'x', 'new_value': 'BRAND_NEW'})
    assert fresh.is_invertible_on(artifact) is True

    colliding = DataFrameOperation(sources=[artifact])
    colliding.chain_operation('fill', {'col_name': 'c', 'old_value': 'x', 'new_value': 'y'})
    assert colliding.is_invertible_on(artifact) is False


def test_a_chain_is_invertible_only_if_every_step_is(tmp_path):
    df = pd.DataFrame({'val': [1, 2, 3, 4] * 5, 'grp': ['a', 'b'] * 10})
    artifact = _artifact(tmp_path, df)
    operation = DataFrameOperation(sources=[artifact])
    operation.chain_operation('rename', {'column_map': {'grp': 'grp2'}})
    operation.chain_operation('apply', {'numeric_col': 'val', 'a': 2, 'b': 0})
    assert operation.is_invertible_on(artifact) is True
    operation.chain_operation('sample', {'frac': 0.5, 'random_state': 1})
    assert operation.is_invertible_on(artifact) is False


def test_equivalence_trivial_all_singletons(tmp_path):
    """A workflow of only lossy operators yields no non-trivial classes."""
    wf = generate_workflow(DataFrameWorkflow, name='eqtriv', num_versions=8,
                           base_shape=(10, 300), out_directory=str(tmp_path), matfreq=1,
                           seed=8, exclude_ops=['apply', 'rename', 'astype', 'fill',
                                                'merge', 'train_test_split'])
    classes = compute_equivalence_classes(wf)
    assert len(set(classes.values())) == len(classes), 'expected all singletons'


def test_equivalence_invertible_chain_forms_one_class(tmp_path):
    """apply -> rename -> apply over three materialized edges gives one class of four."""
    wf = DataFrameWorkflow(name='eqchain', out_directory=str(tmp_path))
    wf.generate_base_artifact(num_rows=50, num_cols=6, rng=1)

    steps = [('apply', {'numeric_col': None, 'a': 2, 'b': 3}),
             ('rename', {'column_map': None}),
             ('apply', {'numeric_col': None, 'a': 5, 'b': 7})]
    for op, args in steps:
        source = wf.artifact_dict[wf.artifact_list[-1]]
        numeric = [c for c in source.to_df().columns
                   if pd.api.types.is_numeric_dtype(source.to_df()[c])]
        assert numeric, 'base artifact has no numeric column to apply over'
        if op == 'apply':
            args = {**args, 'numeric_col': numeric[0]}
        else:
            args = {'column_map': {numeric[0]: f'{numeric[0]}__r'}}
        wf.initialize_operation(artifacts=[source])
        wf.chain_to_current_operation([{'op': op, 'args': args}])
        wf.execute_current_operation(wf.generate_next_label())

    classes = compute_equivalence_classes(wf)
    assert len(wf.artifact_list) == 4
    assert len(set(classes.values())) == 1, f'expected one class, got {classes}'


def test_equivalence_sidecar_is_written(tmp_path):
    wf = _wf(tmp_path)
    with open(f'{wf.out_dir}/{wf.name}_equivalence_classes.json') as infile:
        classes = json.load(infile)
    assert set(classes) == set(wf.artifact_dict)
    assert all(isinstance(v, int) for v in classes.values())


def test_equivalence_class_ids_are_stable_across_runs(tmp_path):
    """Ids must not depend on set iteration order, or two corpus generations are not
    comparable."""
    first = compute_equivalence_classes(_wf(tmp_path / 'a'))
    second = compute_equivalence_classes(_wf(tmp_path / 'b'))
    assert first == second
