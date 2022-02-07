import itertools
import logging

import pytest
import numpy as np

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
               '8b5GZ__uri_page',
               'Vg4hn__name_male',
               'AqhyH__century',
               'mRIWF__postalcode_in_state',
               '9YjpC__credit_card_provider'],
 'numeric': ['zmpoV__randomize_nb_elements']}

operations = [
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
]

@pytest.mark.parametrize('artifact', generated_artifact_fixtures)
def test_sample(artifact, request):
    concrete_artifact = request.getfixturevalue(artifact)
    sample_op = concrete_artifact.operation_class(sources=[concrete_artifact], new_label='sample_df',
                                                  op='sample', args={'frac': 0.5})
    sample_op.execute()


@pytest.mark.parametrize('artifact, op_dict', itertools.product(static_artifact_fixtures, operations))
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
