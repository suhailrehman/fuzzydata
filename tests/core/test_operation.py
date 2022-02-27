import itertools
import logging
import os

import pytest
import numpy as np

from fuzzydata.core.generator import generate_pkfk_join_table
from tests.conftest import static_artifact_fixtures, generated_artifact_fixtures

logger = logging.getLogger(__name__)


_schema_type_mapping = {'string': ['EafKN__rgb_color',
            'qL81j__domain_name',
            'Vyl6E__text'],
 'joinable': ['RFD4U__uuid4',
              'a0UaD__zipcode_in_state',
              'dwdle__zipcode_plus4'],
 'groupable': ['M8OoL__postcode',
               'Qe0kk__ipv4_network_class',
               'dHchx__suffix_female',
               'Vg4hn__name_male',
               'AqhyH__century',
               'mRIWF__postalcode_in_state',
               '9YjpC__credit_card_provider'],
 'numeric': ['zmpoV__randomize_nb_elements']}

_operations = [
    {'op': 'sample',
     'args': {'frac': 0.5}},
    {'op': 'groupby',
     'args': {'group_columns': np.random.choice(_schema_type_mapping['groupable'], 2, replace=False).tolist(),
              'agg_columns': _schema_type_mapping['numeric'],
              'agg_function': 'max'}
     },
    {'op': 'select',
     'args': {'condition': 'zmpoV__randomize_nb_elements > 5'},
     },
    {'op': 'project',
     'args': {'output_cols': np.random.choice(_schema_type_mapping['groupable'], 4)},
     },
    {'op': 'pivot',
     'args': {'index_cols': ['RFD4U__uuid4'],
              'columns': ['9YjpC__credit_card_provider'],
              'value_col': ['zmpoV__randomize_nb_elements'],
              'agg_func': 'sum'},
     },
    {'op': 'apply',
     'args': {'numeric_col': 'zmpoV__randomize_nb_elements',
              'a': 0.5, 'b': 1.0}},
    {'op': 'fill',
     'args': {'col_name': '9YjpC__credit_card_provider',
              'old_value': 'Visa',
              'new_value': 'RuPay'}}
]

_merge_operation = {
    'op': 'merge',
    'args': {'key_col': 'a0UaD__zipcode_in_state'}
}

@pytest.mark.parametrize('artifact', generated_artifact_fixtures)
def test_sample(artifact, request):
    concrete_artifact = request.getfixturevalue(artifact)
    sample_op = concrete_artifact.operation_class(sources=[concrete_artifact], new_label='sample_df',
                                                  op='sample', args={'frac': 0.5})
    sample_op.execute()


@pytest.mark.parametrize('artifact, op_dict', itertools.product(static_artifact_fixtures, _operations))
def test_operations(artifact, request, op_dict):
    try:
        concrete_artifact = request.getfixturevalue(artifact)
        op, args = op_dict['op'], op_dict['args']
        logger.info(f'Testing: {op} operation on {concrete_artifact.__class__} instance')
        sample_op = concrete_artifact.operation_class(sources=[concrete_artifact], new_label=f'after_{op}',
                                                      op=op, args=args)
        sample_op.execute()
    except NotImplementedError as e:
        logger.warning('Warning: {op} operation on {concrete_artifact.__class__} instance not implemented')


@pytest.mark.parametrize('source_artifact', static_artifact_fixtures)
def test_merge_op(source_artifact, request):
    concrete_artifact = request.getfixturevalue(source_artifact)
    new_df, new_schema = generate_pkfk_join_table(source_table=concrete_artifact.to_df(),
                                                  source_schema=concrete_artifact.schema_map,
                                                  key_col=_merge_operation['args']['key_col'])
    extra_args = {}
    if hasattr(concrete_artifact,'sql_engine'):
        extra_args['sql_engine'] = concrete_artifact.sql_engine

    join_artifact = concrete_artifact.__class__('join_df',
                                                filename=os.path.dirname(concrete_artifact.filename) + 'join_df.csv',
                                                from_df=new_df, schema_map=new_schema, **extra_args)

    join_op = concrete_artifact.operation_class(sources=[concrete_artifact, join_artifact],
                                                  new_label='after_join', op=_merge_operation['op'],
                                                  args=_merge_operation['args'],
                                                  artifact_class=concrete_artifact.__class__)

    result_artifact = join_op.execute()

    expected_join_result_cols = set(concrete_artifact.to_df().columns).union(set(join_artifact.to_df().columns))
    assert set(result_artifact.to_df().columns) == expected_join_result_cols

