"""A3 acceptance: profile real tables, generate from a real seed table, fail loudly when a
table admits no operations."""
import logging

import numpy as np
import pandas as pd
import pytest

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.core.generator import generate_workflow, load_seed_table
from fuzzydata.lineage.profiler import (InsufficientSchemaError, describe_table,
                                        infer_column_types, infer_schema_map,
                                        validate_schema_map)

logger = logging.getLogger(__name__)

SEED_CSV = 'tests/fixtures/housing_seed.csv'


def test_profiler_labels_hand_built_frames():
    df = pd.DataFrame({
        'user_id': range(200),                        # unique ints -> identifier, not numeric
        'order_key': [f'K{i}' for i in range(200)],   # unique strings -> joinable
        'country': ['US', 'FR', 'DE', 'JP'] * 50,     # low cardinality -> groupable
        'amount': np.linspace(1.0, 500.0, 200),       # continuous -> numeric
        'comment': [f'free text {i % 7}' for i in range(200)],
    })
    labels = infer_column_types(df)

    # An integer primary key must NOT be treated as a quantity: arithmetic and aggregation
    # over ids are meaningless, and would teach an encoder a spurious signal.
    assert 'numeric' not in labels['user_id']
    assert 'joinable' in labels['user_id']

    assert 'joinable' in labels['order_key']
    assert 'groupable' in labels['country']
    assert 'numeric' in labels['amount']
    assert 'string' in labels['comment']


def test_profiler_handles_nulls_and_empty_columns():
    df = pd.DataFrame({
        'all_null': [None] * 50,
        'half_null': [1.0, None] * 25,
        'grp': ['a', 'b'] * 25,
    })
    labels = infer_column_types(df)
    assert labels['all_null'] == ['string']          # opaque passenger column
    assert 'numeric' in labels['half_null']


def test_schema_map_has_one_provider_per_column():
    df = pd.read_csv(SEED_CSV)
    schema_map = infer_schema_map(df)
    assert set(schema_map) == {str(c) for c in df.columns}
    assert all(isinstance(v, str) for v in schema_map.values())


def test_insufficient_schema_raises_and_names_the_gap():
    """An all-string, all-unique table admits no operation. It must fail up front with the
    reason, not deep inside the generator."""
    df = pd.DataFrame({'a': [f'x{i}' for i in range(50)],
                       'b': [f'y{i}' for i in range(50)]})
    schema_map = infer_schema_map(df)
    with pytest.raises(InsufficientSchemaError) as excinfo:
        validate_schema_map(schema_map, df)
    assert 'numeric' in excinfo.value.missing


def test_load_seed_table_csv_and_parquet(tmp_path):
    df = pd.read_csv(SEED_CSV)
    parquet = tmp_path / 'seed.parquet'
    df.to_parquet(parquet, index=False)

    assert load_seed_table(SEED_CSV).shape == df.shape
    assert load_seed_table(parquet).shape == df.shape
    with pytest.raises(ValueError, match='Unsupported seed table format'):
        load_seed_table(tmp_path / 'seed.xlsx')


def test_generate_from_real_table(tmp_path):
    """The headline A3 behaviour: a workflow whose base artifact is a real table."""
    wf = generate_workflow(DataFrameWorkflow, name='real', num_versions=8,
                           out_directory=str(tmp_path), matfreq=1, seed=5,
                           base_artifact=SEED_CSV)
    seed_df = pd.read_csv(SEED_CSV)
    base = wf.artifact_dict[wf.artifact_list[0]].to_df()

    # Base artifact is the real table, not a Faker fabrication.
    assert list(base.columns) == list(seed_df.columns)
    assert len(base.index) == len(seed_df.index)
    assert wf.graph.number_of_nodes() == 8
    # Real column names survive into the derived artifacts.
    assert any('price_usd' in (a.schema_map or {})
               for a in wf.artifact_dict.values())


def test_base_shape_is_ignored_not_an_error(tmp_path, caplog):
    """base_shape must warn, not raise, when a seed table is supplied."""
    with caplog.at_level(logging.WARNING):
        wf = generate_workflow(DataFrameWorkflow, name='warn', num_versions=4,
                               base_shape=(99, 99), out_directory=str(tmp_path),
                               matfreq=1, seed=5, base_artifact=SEED_CSV)
    assert 'ignored' in caplog.text
    base = wf.artifact_dict[wf.artifact_list[0]].to_df()
    assert len(base.columns) != 99


def test_real_seed_generation_is_reproducible(tmp_path):
    def ops(path):
        wf = generate_workflow(DataFrameWorkflow, name='r', num_versions=6,
                               out_directory=str(path), matfreq=2, seed=99,
                               base_artifact=SEED_CSV)
        return [o['op'] for op in wf.operation_list for o in op['op_list']]
    assert ops(tmp_path / 'a') == ops(tmp_path / 'b')


# ---- describe_table tests (issue #19) -----------------------------------------------

def test_describe_table_buckets():
    """Hand-built frames land in the expected row_bucket, col_bucket, and type_mix."""
    # Small numeric-dominant table: use float values so no column is misclassified as joinable
    rng = np.random.default_rng(0)
    small_num = pd.DataFrame({f'n{i}': rng.standard_normal(500) for i in range(6)})
    d = describe_table(small_num)
    assert d['n_rows'] == 500
    assert d['n_cols'] == 6
    assert d['row_bucket'] == '1e2'
    assert d['col_bucket'] == '<10'
    assert d['type_mix'] == 'numeric-dominant'
    assert d['admits_generation'] is True

    # Medium categorical-dominant table (low-cardinality strings = groupable)
    n = 10_000
    med_cat = pd.DataFrame({
        'cat1': (['a', 'b'] * (n // 2))[:n],
        'cat2': (['x', 'y', 'z'] * (n // 3 + 1))[:n],
        'cat3': (['p', 'q'] * (n // 2))[:n],
    })
    d2 = describe_table(med_cat)
    assert d2['row_bucket'] == '1e4'

    # Wide table
    wide = pd.DataFrame({f'c{i}': range(50) for i in range(60)})
    assert describe_table(wide)['col_bucket'] == '>50'

    # 10–50 col bucket
    mid = pd.DataFrame({f'c{i}': range(50) for i in range(25)})
    assert describe_table(mid)['col_bucket'] == '10-50'

    # Large table row bucket
    large = pd.DataFrame({'a': range(200_000), 'b': [1.0] * 200_000})
    assert describe_table(large)['row_bucket'] == '1e6'


def test_describe_table_flags_insufficient():
    """A table with no legal operations reports admits_generation=False without raising."""
    # One text-only column: no numeric column → InsufficientSchemaError internally
    df = pd.DataFrame({'name': ['alice', 'bob', 'carol'] * 10})
    d = describe_table(df)
    assert d['admits_generation'] is False
    # Must not raise
    assert isinstance(d, dict)
