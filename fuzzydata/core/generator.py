import os
import string
import pandas as pd
import numpy as np
import logging

from functools import partial
from typing import Callable, Dict
from loguru import logger
from faker import Faker
from itertools import chain

# Disable Faker log spam in DEBUG mode
logging.getLogger('faker').setLevel(logging.ERROR)

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_UNIQUE_DICTIONARY = string.ascii_letters+string.digits


def load_function_dict(directory=_THIS_DIR+'/config/'):
    return {
        'joinable': [line.rstrip('\n') for line in open(directory + 'joinable_cols.txt')],
        'groupable': [line.rstrip('\n') for line in open(directory + 'groupable_cols.txt')],
        'numeric': [line.rstrip('\n') for line in open(directory + 'numeric_cols.txt')],
        'string': [line.rstrip('\n') for line in open(directory + 'string_cols.txt')],
    }


_gen_functions = load_function_dict()
logger.info(_gen_functions)
_faker_cols = list(set(chain(*_gen_functions.values())))


def generate_prefix(symbol_dict: str, size: int=5) -> str:
    return ''.join(np.random.choice(list(symbol_dict), size))


def generate_table(num_rows: int=100, column_dict: Dict=None) -> pd.DataFrame:
    faker = Faker()
    logger.info(f'Generating base df with {num_rows} rows and {len(column_dict.keys())} columns')
    logger.trace(f'Column list: {column_dict.keys()}')
    series_list = []
    label_list = []
    for label, column in column_dict.items():
        series_list.append(pd.Series((faker.format(column) for _ in range(num_rows))))
        label_list.append(label)
    return pd.concat(series_list, axis=1, keys=label_list)


def generate_schema(num_cols: int, unique_prefix: Callable = partial(generate_prefix, _UNIQUE_DICTIONARY, size=5)) -> Dict[str, str]:
    column_dict = {}
    random_selection = np.random.choice(_faker_cols, size=num_cols)
    column_dict.update({f'{unique_prefix()}__{r}': r for r in random_selection})

    logger.trace(f'Selected columns for this schema: {column_dict.values()}')

    # # Do not need inverse schema maps yet...
    # for col, faker_type in column_dict.items():
    #     for col_type in self.inv_functions[faker_type]:
    #         self.inv_coltype_mapping[col_type].append(col)

    # logger.debug(f'Inverse ColumnType Mapping: {self.inv_coltype_mapping}')

    return column_dict
