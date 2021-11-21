import pandas as pd
import pytest

from fuzzydata.core.generator import generate_schema, generate_table


@pytest.fixture(scope="module", params=[10, 15,20])
def schema(request):
    generated_schema = generate_schema(request.param)
    for col_name, faker_col_name in generated_schema.items():
        assert isinstance(col_name, str)
        prefix, fake_name = col_name.split('__')
        assert fake_name == faker_col_name
    return generated_schema


@pytest.mark.parametrize('num_rows', [10,100,1000])
def test_generate_table(schema, num_rows):
    table = generate_table(num_rows, column_dict=schema)
    assert isinstance(table, pd.DataFrame)
    assert num_rows == len(table.index)





