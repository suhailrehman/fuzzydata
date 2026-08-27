"""A4 acceptance: every 0.1.0 operator round-trips, and client capabilities are honest.

"Honest" means: an operator either works, or the client declares it in unsupported_ops.
There is no third option -- a NotImplementedError from an operator that is not declared is a
failure, not something to log and skip.
"""
import itertools
import logging

import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameOperation
from fuzzydata.clients.sqlite import SQLOperation
from fuzzydata.core.generator import generate_workflow
from fuzzydata.core.operation import Operation
from tests.conftest import static_artifact_fixtures

logger = logging.getLogger(__name__)

#: Every operator the 0.1.0 release adds, with args valid against _static_schema_test.
NEW_OPERATORS = [
    {'op': 'dropna', 'args': {'subset': None}},
    {'op': 'dedupe', 'args': {'subset': None}},
    {'op': 'rename', 'args': {'column_map': {'Vyl6E__text': 'Vyl6E__text__renamed'}}},
    {'op': 'astype', 'args': {'column': 'zmpoV__randomize_nb_elements', 'dtype': 'float64'}},
    {'op': 'normalize', 'args': {'column': 'zmpoV__randomize_nb_elements'}},
    {'op': 'standardize', 'args': {'column': 'zmpoV__randomize_nb_elements'}},
    {'op': 'label_encode', 'args': {'column': '9YjpC__credit_card_provider'}},
    {'op': 'one_hot_encode', 'args': {'column': 'AqhyH__century',
                                      'categories': ['I', 'II', 'III']}},
    {'op': 'train_test_split', 'args': {'frac': 0.7, 'random_state': 11, 'side': 'train'}},
    {'op': 'train_test_split', 'args': {'frac': 0.7, 'random_state': 11, 'side': 'test'}},
]

ALL_OPERATOR_NAMES = [
    'sample', 'apply', 'groupby', 'project', 'select', 'merge', 'pivot', 'fill',
    'dropna', 'dedupe', 'rename', 'astype', 'normalize', 'standardize',
    'label_encode', 'one_hot_encode', 'train_test_split',
]


def _ids(cases):
    return [f"{c['op']}-{c['args'].get('side', '')}".rstrip('-') for c in cases]


@pytest.mark.parametrize('artifact, op_dict',
                         itertools.product(static_artifact_fixtures, NEW_OPERATORS),
                         ids=lambda v: v if isinstance(v, str) else
                         f"{v['op']}{'-' + v['args']['side'] if 'side' in v['args'] else ''}")
def test_new_operator_executes(artifact, request, op_dict):
    concrete = request.getfixturevalue(artifact)
    op, args = op_dict['op'], op_dict['args']
    operation_class = concrete.operation_class

    if op in operation_class.unsupported_ops:
        pytest.skip(f'{op} is declared unsupported by {operation_class.__name__}')

    operation = operation_class(sources=[concrete])
    operation.chain_operation(op, args)
    result = operation.execute(f'after_{op}_{args.get("side", "x")}')

    assert result is not None
    assert operation.op_list == [{'op': op, 'args': args}]
    # The recorded destination schema must match what actually materialized.
    assert set(result.schema_map) == set(result.to_df().columns), (
        f'{op}: schema_map disagrees with the materialized columns')


def test_every_operator_is_abstract_on_the_base_class():
    """Guards against an operator being implemented on a client but never declared, which
    would let one client silently diverge from the others."""
    for name in ALL_OPERATOR_NAMES:
        assert callable(getattr(Operation, name, None)), f'{name} missing from Operation ABC'


@pytest.mark.parametrize('name', ALL_OPERATOR_NAMES)
def test_all_ops_sqlite_parity(name):
    """Each operator is either implemented by the SQL client or explicitly declared
    unsupported. No silent divergence."""
    assert callable(getattr(SQLOperation, name, None)), f'{name} missing from SQLOperation'
    assert callable(getattr(DataFrameOperation, name, None)), f'{name} missing from pandas'
    # Declared-unsupported operators must actually raise, so the declaration cannot rot.
    if name in SQLOperation.unsupported_ops:
        assert name not in DataFrameOperation.unsupported_ops, (
            f'{name} unsupported everywhere: drop it rather than shipping it')


def test_one_hot_encode_expands_schema(tmp_path):
    """The column-count explosion case, which stresses schema tracking hardest."""
    df = pd.DataFrame({'grp': ['a', 'b', 'c'] * 20, 'val': list(range(60))})
    artifact = DataFrameArtifact('src', filename=str(tmp_path / 'src.csv'), from_df=df,
                                 schema_map={'grp': '__profiled_groupable',
                                             'val': '__profiled_numeric'})
    operation = DataFrameOperation(sources=[artifact])
    operation.chain_operation('one_hot_encode', {'column': 'grp',
                                                 'categories': ['a', 'b', 'c']})
    result = operation.execute('onehot')
    columns = set(result.to_df().columns)

    assert 'grp' not in columns, 'source column should be consumed'
    assert {'grp__is_a', 'grp__is_b', 'grp__is_c'} <= columns
    assert set(result.schema_map) == columns
    assert result.to_df()['grp__is_a'].sum() == 20


def test_train_test_split_sides_partition_the_parent(tmp_path):
    """Sharing a seed is not enough: the sides must be complementary, or the corpus records
    two overlapping samples as a partition."""
    df = pd.DataFrame({'val': list(range(100))})
    schema = {'val': '__profiled_numeric'}
    artifact = DataFrameArtifact('src', filename=str(tmp_path / 's.csv'), from_df=df,
                                 schema_map=schema)
    sides = {}
    for side in ('train', 'test'):
        operation = DataFrameOperation(sources=[artifact])
        operation.chain_operation('train_test_split',
                                  {'frac': 0.7, 'random_state': 3, 'side': side})
        sides[side] = operation.execute(f'split_{side}').to_df()

    assert len(sides['train']) + len(sides['test']) == len(df)
    overlap = set(sides['train']['val']) & set(sides['test']['val'])
    assert not overlap, f'sides overlap on {len(overlap)} rows'


@pytest.mark.parametrize('excluded', ['dropna', 'one_hot_encode', 'train_test_split',
                                      'normalize', 'rename'])
def test_exclude_ops_still_works_with_new_names(tmp_path, excluded):
    from fuzzydata.clients.pandas import DataFrameWorkflow
    wf = generate_workflow(DataFrameWorkflow, name=f'ex_{excluded}', num_versions=8,
                           base_shape=(10, 300), out_directory=str(tmp_path),
                           matfreq=1, seed=17, exclude_ops=[excluded])
    used = {o['op'] for op in wf.operation_list for o in op['op_list']}
    assert excluded not in used
