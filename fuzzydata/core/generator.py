import itertools
import math
import os
import string
from collections import defaultdict

import pandas
import numpy as np
import logging

from functools import partial
from typing import Callable, Collection, Dict, List, Optional

import pandas as pd
from faker import Faker
from itertools import chain

from fuzzydata.lineage.profiler import PROFILED_PREFIX, is_profiled_provider
from fuzzydata.lineage.validity import MIN_COLUMNS, MIN_ROWS


logging.getLogger('faker').setLevel(logging.ERROR)
logger = logging.getLogger(__name__)


#: Fallback RNG for callers that do not supply one. Every stochastic helper below accepts an
#: explicit `rng`; generate_workflow() always passes one so a seeded run is fully determined.
#: Note this is a np.random.Generator, NOT the legacy global np.random state -- the legacy
#: global is shared across multiprocessing forks and cannot give per-workflow determinism.
_DEFAULT_RNG = np.random.default_rng()

#: Upper bound for seeds handed to third parties (Faker, pandas random_state).
_SEED_MAX = 2 ** 32

#: Cap on one_hot_encode's category count, to bound the column-count explosion.
ONE_HOT_MAX_CATEGORIES = 8

#: train_test_split needs enough rows that neither side comes out empty.
TRAIN_TEST_SPLIT_MIN_ROWS = 20

#: pivot emits one column per distinct value of its `columns` argument. Beyond this the
#: artifact is a degenerate wide reshape rather than a useful corpus member.
PIVOT_MAX_OUTPUT_COLUMNS = 32


def coerce_rng(seed_or_rng=None) -> np.random.Generator:
    """Normalise None | int | Generator into a Generator.
    :param seed_or_rng: None (use the module default), an int seed, or a Generator.
    :return: np.random.Generator
    """
    if seed_or_rng is None:
        return _DEFAULT_RNG
    if isinstance(seed_or_rng, np.random.Generator):
        return seed_or_rng
    return np.random.default_rng(seed_or_rng)


def draw_seed(rng: np.random.Generator = None) -> int:
    """Draw a concrete integer seed to hand to Faker or pandas' random_state. Recording this
    value is what makes a stochastic operation replayable."""
    return int(coerce_rng(rng).integers(0, _SEED_MAX))


_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_UNIQUE_DICTIONARY = string.ascii_letters+string.digits


def load_function_dict(directory=_THIS_DIR+'/config/'):
    return {
        'joinable': [line.rstrip('\n') for line in open(directory + 'joinable_cols.txt')],
        'groupable': [line.rstrip('\n') for line in open(directory + 'groupable_cols.txt')],
        'numeric': [line.rstrip('\n') for line in open(directory + 'numeric_cols.txt')],
        'string': [line.rstrip('\n') for line in open(directory + 'string_cols.txt')],
    }


def generate_inverse_function_dict(function_dict):
    inv_functions = defaultdict(list)
    for k, vs in function_dict.items():
        for v in vs:
            inv_functions[v].append(k)
    return inv_functions


_gen_functions = load_function_dict()
logger.debug(_gen_functions)
# sorted(), not list(set(...)): set iteration order over strings is randomised per process
# by PYTHONHASHSEED, so an unsorted list made generate_schema() pick different providers for
# the same seed in different processes. That silently broke the cross-process reproducibility
# that --seed and the corpus driver both promise -- and it hid from a same-process test,
# because forked corpus workers inherit the parent's hash seed.
_faker_cols = sorted(set(chain(*_gen_functions.values())))
_inv_gen_functions = generate_inverse_function_dict(_gen_functions)


def generate_prefix(symbol_dict: str, size: int = 5, rng: np.random.Generator = None) -> str:
    return ''.join(coerce_rng(rng).choice(list(symbol_dict), size))


def generate_table(num_rows: int = 100, column_dict: Dict = None, pd=pandas, key_series=None,
                   rng: np.random.Generator = None) -> pandas.DataFrame:
    """
    Generate a table with a given schema and number of rows
    :param num_rows: Number of rows desired in the table
    :param column_dict: Schema Mapping (column_label->faker_provider) as a Dict
    :param pd: pandas library to be used to generated (default pandas), you can also use modin.pandas
    :param key_series: A pd.Series object that contains a key column to be left-appended to the df. Overrides num_rows.
    :param rng: Generator used to derive the Faker seed, so generated values are reproducible.
    :return: Dataframe with generated table according to spec.
    """
    faker = Faker()
    # seed_instance (not the Faker.seed classmethod) keeps the seeding local to this call.
    faker.seed_instance(draw_seed(rng))

    series_list = []
    label_list = []

    if key_series is not None:
        series_list.append(key_series)
        label_list.append(key_series.name)
        logger.info(f'Generating right-merge df df with {num_rows} rows and {len(column_dict.keys())} columns')
    else:
        logger.info(f'Generating base df with {num_rows} rows and {len(column_dict.keys())} columns')

    for label, column in column_dict.items():
        series_list.append(pd.Series((faker.format(column) for _ in range(num_rows))))
        label_list.append(label)

    logger.debug(f'Column list: {label_list}')
    return pd.concat(series_list, axis=1, keys=label_list)


def generate_schema(num_cols: int, unique_prefix: Callable = None,
                   rng: np.random.Generator = None) -> Dict[str, str]:
    """
    Generates a randomized schema given number of columns.
    :param num_cols: Number of columns to generate.
    :param unique_prefix: A function that generates a unique column prefix (default is 5 char random string).
    :param rng: Generator to draw from, so the schema is reproducible under a seed.
    :return: Dict of column_label->faker provider as per spec.
    """
    rng = coerce_rng(rng)
    if unique_prefix is None:
        unique_prefix = partial(generate_prefix, _UNIQUE_DICTIONARY, size=5, rng=rng)
    column_dict = {}
    num_col_types = len(_gen_functions.keys())
    if num_cols < num_col_types:
        random_selection = rng.choice(_faker_cols, size=num_cols)
    else:
        # Better randomization of columns to ensure at least one of each type are generated
        random_selection = []
        num_array = np.ones(num_col_types, dtype=int)
        while sum(num_array) < num_cols:
            ix = rng.integers(0, num_col_types)
            num_array[ix] += 1
        for ix, col_type in enumerate(_gen_functions.keys()):
            random_selection.extend(rng.choice(_gen_functions[col_type], size=num_array[ix]))

    logger.debug(random_selection)
    column_dict.update({f'{unique_prefix()}__{r}': r for r in random_selection})
    logger.debug(f'Selected columns for this schema: {column_dict.values()}')
    return column_dict


def get_schema_type_mapping(column_dict):
    # Do not need inverse schema maps yet...
    schema_type_mapping = defaultdict(list)
    for col, faker_type in (column_dict or {}).items():
        if is_profiled_provider(faker_type):
            # Columns profiled from a real seed table carry a sentinel provider naming their
            # label directly, since a real table has no Faker provider to look up.
            col_types = [faker_type[len(PROFILED_PREFIX):]]
        else:
            col_types = _inv_gen_functions[faker_type]
        for col_type in col_types:
            schema_type_mapping[col_type].append(col)

    logger.debug(f'Inverse ColumnType Mapping: {schema_type_mapping}')

    return schema_type_mapping


def select_rand_cols(df_col_types, num, col_type=None, rng: np.random.Generator = None):
    """
    Select a random "num" of columns from a given column_name: type mapping
    :param df_col_types: Mapping of column names to types.
    :param num: Number of columns required
    :param col_type: Column types required
    :param rng: Generator to draw from.
    :return:
    """
    if not col_type:
        all_options = list(itertools.chain(df_col_types.values()))
    else:
        all_options = df_col_types[col_type]
    try:
        logger.debug(f'Selection Options for {col_type} type: {all_options}')
        options = coerce_rng(rng).choice(all_options, num, replace=False).tolist()
    except ValueError as e:
        logger.warning(f'Could not select {num} columns of type {col_type}')
        return None
    return options


def select_rand_aggregate(rng: np.random.Generator = None):
    return coerce_rng(rng).choice(['min', 'max', 'sum', 'mean', 'count'], 1)[0]


def get_rand_percentage(minimum=0.1, maximum=0.99, rng: np.random.Generator = None):
    return round((maximum - minimum) * coerce_rng(rng).random() + minimum,  2)


def generate_pkfk_join_table(source_table, source_schema: Dict['str', 'str'],
                             key_col: str, new_col_size=None, pd=pandas,
                             rng: np.random.Generator = None):
    """
    Generates a randomized PK-FK table (right table) for a merge/join operation, given a source schema and key_column.
    :param source_table: Source table to be joined.
    :param source_schema: Source Schema.
    :param key_col: Column Label to be used as a key.
    :param new_col_size: Number of columns required for the new table .
    :param pd: pandas library to be used.
    :return:
    """
    rng = coerce_rng(rng)
    # sorted() so the key order does not depend on set iteration order, which varies with
    # PYTHONHASHSEED and would make generation irreproducible even under a fixed seed.
    key_values = sorted(set(source_table[key_col].values), key=_stable_sort_key)
    key_series = pd.Series(data=key_values, name=key_col)
    if not new_col_size:
        new_col_size = rng.integers(2, max(3, len(source_table.columns)+1))

    new_schema = generate_schema(new_col_size, rng=rng)
    new_df = generate_table(num_rows=len(key_series.index), column_dict=new_schema, pd=pd,
                            key_series=key_series, rng=rng)
    new_schema[key_col] = source_schema[key_col]

    return new_df, new_schema


def _group_cardinality(source_df, columns) -> int:
    """Number of distinct groups `columns` would produce, or a large sentinel when the data
    is not available (in which case the caller allows the operation, as before)."""
    if source_df is None or not columns:
        return 1 << 30
    present = [c for c in columns if c in getattr(source_df, 'columns', [])]
    if len(present) != len(columns):
        return 1 << 30
    try:
        return int(source_df[present].drop_duplicates().shape[0])
    except (TypeError, ValueError):
        return 1 << 30


def _stable_sort_key(value):
    """Total order across mixed types. A column's values are not guaranteed homogeneous
    (Faker providers are not type-homogeneous, and real seed tables have object columns), so
    a bare sorted() raises TypeError comparing e.g. int to str. Sorting by (type name, text)
    is deterministic, which is all we need -- the goal is reproducibility, not collation."""
    return (type(value).__name__, str(value))


def _py_scalar(value):
    """Unbox a numpy scalar to a plain Python one. Operation args are JSON-serialized into
    the workflow spec, and json cannot encode np.int64/np.float64."""
    item = getattr(value, 'item', None)
    return item() if callable(item) else value


def _numeric_series(source_df, col):
    """Coerce a column of the source table to numeric, or None if it is not numeric.
    Faker providers emit strings, so the declared 'numeric' type does not guarantee dtype."""
    if source_df is None or col not in getattr(source_df, 'columns', []):
        return None
    try:
        series = pd.to_numeric(source_df[col], errors='coerce').dropna()
    except (TypeError, ValueError):
        return None
    return series if len(series) else None


def load_seed_table(path) -> pandas.DataFrame:
    """Read a real seed table from disk. Supports .csv and .parquet.
    :param path: path to the seed table.
    :return: DataFrame
    """
    path = str(path)
    suffix = os.path.splitext(path)[1].lower()
    if suffix in ('.parquet', '.pq'):
        return pd.read_parquet(path)
    if suffix in ('.csv', '.txt', ''):
        return pd.read_csv(path)
    raise ValueError(f'Unsupported seed table format {suffix!r} for {path}; '
                     f'expected .csv or .parquet')


def generate_ops_choices(schema: Dict[str, str], num_rows: int,
                         exclude: Optional[Collection[str]] = None,
                         rng: np.random.Generator = None,
                         source_df=None) -> List[Dict]:
    """
    Generate a number of options for the next operation to be performed on a given table.
    :param schema: Column Map
    :param num_rows: number of rows in the table
    :param exclude: operation names to leave out (client capabilities + caller exclusions)
    :param rng: Generator to draw arguments from, so choices are reproducible under a seed
    :param source_df: (optional) the source table. Enables data-aware operations -- `select`
        needs a threshold that does not filter everything out, and `fill` needs a value that
        actually occurs in the column, or the operation is a silent no-op. Without it those
        two operations are simply not offered.
    :return: List of {'op': str, 'args': dict} choices
    """
    rng = coerce_rng(rng)
    ops_choices = []
    df_col_types = get_schema_type_mapping(schema)

    logger.debug(f"df_col_types: {df_col_types}")
    '''
    OPS REQUIREMENTS -- schema preconditions for each generatable operation.

      apply   = one numeric col; emits a derived ax+b column (schema-growing, invertible
                for a != 0)
      groupby = one groupable col, at least one numeric col, random aggregation function
      pivot   = two groupable cols (index + columns) and one numeric values col. Only
                emitted as the last op in a chain (it blanks the schema map until
                materialization) and unsupported by the sql client.
      merge   = at least one joinable col. The right-hand table is synthesised by
                generate_pkfk_join_table(), not drawn from existing artifacts.
      sample  = random DF fraction, minimum 10 rows. Carries a concrete random_state.
      project = at least 3 columns in the schema
      select  = a numeric col AND source_df, so the threshold can be a real quantile
      fill    = any col AND source_df, so old_value is a value that actually occurs
    '''

    if 'numeric' in df_col_types:
        numeric_col = select_rand_cols(df_col_types, 1, 'numeric', rng=rng)[0]

        # apply: linear ax+b on a numeric column. Integer scalars keep the derived column
        # name a valid Python identifier, which matters because the pandas client emits
        # `.assign(<name> = ...)` and that string is eval()'d.
        a, b = (int(x) for x in rng.integers(1, 100, 2))
        ops_choices.append({'op': 'apply',
                            'args': {'numeric_col': numeric_col, 'a': a, 'b': b}})

        # select: filter rows on a numeric threshold. Use a low quantile of the real data so
        # the result is non-degenerate -- a blind threshold can easily match zero rows.
        series = _numeric_series(source_df, numeric_col)
        if (series is not None and num_rows >= MIN_ROWS * 2
                and float(series.min()) < float(series.max())):
            # Threshold must sit strictly below the maximum, or the filter matches nothing.
            # A constant column is skipped entirely for the same reason -- `col > c` where
            # every value equals c yields an empty artifact, which is corpus poison.
            threshold = float(series.quantile(0.25))
            if threshold >= float(series.max()):
                threshold = float(series.min())
            # Backticks: column labels may start with a digit, which is an unrecognised
            # token unquoted in both SQLite and pandas .query(). Both accept backticks.
            ops_choices.append({'op': 'select',
                                'args': {'condition': f'`{numeric_col}` > {threshold}'}})

        if 'groupable' in df_col_types:
            # groupby, pivots now possible
            num_groups = min(int(rng.integers(1, 3)), len(df_col_types['groupable']))
            group_cols = select_rand_cols(df_col_types, num_groups, 'groupable', rng=rng)
            func = select_rand_aggregate(rng=rng)
            # groupby emits one row per distinct group. Faker's "groupable" providers include
            # very low cardinality ones (a 3-value column collapses a 300-row artifact to 3
            # rows), which is the single largest source of degenerate artifacts. Require
            # enough groups to clear the validity floor when we can see the data.
            if _group_cardinality(source_df, group_cols) >= MIN_ROWS:
                ops_choices.append({'op': 'groupby',
                                    'args': {'group_columns': group_cols,
                                             'agg_columns': df_col_types['numeric'],
                                             'agg_function': func}
                                    })

            # pivot selections
            if len(df_col_types['groupable']) >= 2:
                index, columns = select_rand_cols(df_col_types, 2, 'groupable', rng=rng)
                values = numeric_col
                # pivot emits one column per distinct value of `columns`. Faker's
                # "groupable" providers are not all low-cardinality, so this could explode a
                # 10-column parent into hundreds of columns. Skip when we can see that it
                # would; without source_df we cannot tell, so allow it as before.
                pivot_width = (source_df[columns].nunique()
                               if source_df is not None
                               and columns in getattr(source_df, 'columns', []) else 0)
                pivot_height = _group_cardinality(source_df, [index])
                if pivot_height < MIN_ROWS:
                    logger.debug(f'Skipping pivot on index {index}: only {pivot_height} '
                                 f'distinct values, so the result would have too few rows')
                elif pivot_width > PIVOT_MAX_OUTPUT_COLUMNS:
                    logger.debug(f'Skipping pivot on {columns}: would emit {pivot_width} '
                                 f'columns (limit {PIVOT_MAX_OUTPUT_COLUMNS})')
                else:
                    ops_choices.append({'op': 'pivot',
                                        'args': {'index_cols': [index], 'columns': [columns],
                                                 'value_col': [values],
                                                 'agg_func': select_rand_aggregate(rng=rng)}
                                        })

    if 'joinable' in df_col_types:
        on = select_rand_cols(df_col_types, 1, 'joinable', rng=rng)[0]
        ops_choices.append({'op': 'merge', 'args': {'key_col': on}})

    # fill: replace an existing value with a freshly faked one of the same provider type.
    # Values are passed RAW -- each client quotes them for its own target language, because
    # the pandas client eval()s its code while the sql client needs SQL literals.
    if source_df is not None:
        fill_col = select_rand_cols({'all': sorted(schema)}, 1, 'all', rng=rng)
        if fill_col:
            fill_col = fill_col[0]
            if fill_col in getattr(source_df, 'columns', []):
                present = source_df[fill_col].dropna()
                if len(present):
                    old_value = _py_scalar(present.iloc[int(rng.integers(0, len(present)))])
                    provider = schema[fill_col]
                    if is_profiled_provider(provider):
                        # Real seed tables have no Faker provider. Draw a different existing
                        # value so fill stays a genuine substitution rather than a no-op.
                        distinct = present[present != old_value]
                        new_value = _py_scalar(
                            distinct.iloc[int(rng.integers(0, len(distinct)))]
                        ) if len(distinct) else old_value
                    else:
                        faker = Faker()
                        faker.seed_instance(draw_seed(rng))
                        try:
                            new_value = faker.format(provider)
                        except (AttributeError, TypeError):
                            new_value = old_value
                    ops_choices.append({'op': 'fill',
                                        'args': {'col_name': fill_col,
                                                 'old_value': old_value,
                                                 'new_value': new_value}})

    # ---- 0.1.0 operators ----------------------------------------------------------
    all_columns = sorted(schema)

    # Schema-preserving but cardinality-reducing, so they need headroom above the floor.
    if num_rows >= MIN_ROWS * 2:
        ops_choices.append({'op': 'dropna', 'args': {'subset': None}})
        ops_choices.append({'op': 'dedupe', 'args': {'subset': None}})

    if all_columns:
        target = all_columns[int(rng.integers(0, len(all_columns)))]
        ops_choices.append({'op': 'rename',
                            'args': {'column_map': {target: f'{target}__renamed'}}})

    # astype only on numeric columns, and only widening casts. Casting a free-text column to
    # a number yields nulls (pandas) or zeroes (SQLite) -- a silent corruption, not a cast.
    if 'numeric' in df_col_types:
        cast_col = select_rand_cols(df_col_types, 1, 'numeric', rng=rng)[0]
        ops_choices.append({'op': 'astype', 'args': {'column': cast_col, 'dtype': 'float64'}})
        ops_choices.append({'op': 'normalize', 'args': {'column': cast_col}})
        ops_choices.append({'op': 'standardize', 'args': {'column': cast_col}})

    if 'groupable' in df_col_types:
        enc_col = select_rand_cols(df_col_types, 1, 'groupable', rng=rng)[0]
        ops_choices.append({'op': 'label_encode', 'args': {'column': enc_col}})

        # one_hot_encode needs the concrete category list: the destination schema has to be
        # known from the spec alone, so it cannot be inferred from data at replay time.
        # Capped to keep the column-count explosion bounded.
        if source_df is not None and enc_col in getattr(source_df, 'columns', []):
            distinct = source_df[enc_col].dropna().unique()
            if 0 < len(distinct) <= ONE_HOT_MAX_CATEGORIES:
                categories = sorted((_py_scalar(c) for c in distinct), key=_stable_sort_key)
                ops_choices.append({'op': 'one_hot_encode',
                                    'args': {'column': enc_col, 'categories': categories}})

    # train_test_split needs enough rows for both sides to be non-empty.
    if num_rows >= TRAIN_TEST_SPLIT_MIN_ROWS:
        ops_choices.append({'op': 'train_test_split',
                            'args': {'frac': get_rand_percentage(0.5, 0.9, rng=rng),
                                     'random_state': draw_seed(rng)}})

    if num_rows >= 10:
        frac = get_rand_percentage(rng=rng)
        # Repeated sampling compounds: a chain of fracs shrinks an artifact fast. Only offer
        # the draw when the result still clears the validity floor.
        if math.ceil(num_rows * frac) >= MIN_ROWS:
            # Draw the seed now and record it, so replay reproduces the same rows.
            ops_choices.append({'op': 'sample',
                                'args': {'frac': frac, 'random_state': draw_seed(rng)}})

    # NB: this is the number of columns KEPT, despite the historical name. Floored at
    # MIN_COLUMNS: projecting down to a single column leaves an artifact with essentially no
    # distinguishing content.
    if len(schema) > MIN_COLUMNS:
        num_keep = int(rng.integers(MIN_COLUMNS, len(schema)))
        ops_choices.append({'op': 'project',
                            'args': {
                                'output_cols': rng.choice(sorted(schema), num_keep, replace=False).tolist()
                            }
                            })

    # Filter exclusion list of ops here
    exclude = exclude or ()
    ops_choices = list(filter(lambda x: x['op'] not in exclude, ops_choices))

    return ops_choices


def _generate_sibling_split(wf, source_artifact, args, rng):
    """Materialize both sides of a train/test split as sibling operations.

    Operation is single-destination, so a two-output operator has to be modelled as two
    operations. They share `random_state` AND a `sibling_group` id, which is what lets a
    consumer recover that the two children partition their parent rather than being two
    independent derivations. Making Operation genuinely multi-destination is Track B (B2).
    """
    sibling_group = f'{wf.name}_split_{draw_seed(rng)}'
    for side in ('train', 'test'):
        wf.initialize_operation(artifacts=[source_artifact])
        wf.current_operation.sibling_group = sibling_group
        wf.chain_to_current_operation([{'op': 'train_test_split',
                                        'args': {**args, 'side': side}}])
        wf.execute_current_operation(wf.generate_next_label())


def generate_workflow(workflow_class, name='wf', num_versions=10, base_shape=(10, 1000),
                      out_directory='/tmp/dataset', bfactor=1.0, matfreq=1, wf_options=None,
                      exclude_ops=None, seed=None, topology='bfactor', base_artifact=None,
                      validate='warn', file_format='csv',
                      operator_policy='schema_constrained', idiom=None):
    """
    Generate a workflow for a given client and parameters
    :param workflow_class: Workflow class to be used (DataFrameWorkflow, ModinWorkflow, or SQLWorkflow)
    :param name: Name for the workflow (Default: 'wf')
    :param num_versions: Number of artifacts to generate (Default 10).
    :param base_shape: tuple of (columns, rows) to generate as the first artifact. Default is (10,1000).
    :param out_directory: output directory to use for generation
    :param bfactor: branch factor for workflow graph (default 1.0)
    :param matfreq: Number of operations to perform before materialization (default 1)
    :param wf_options: Workflow class options as a dict (e.g. SQL string or Modin engine)
    :param exclude_ops: List of string operations to be avoided during generation.
    :param seed: int seed, or an np.random.Generator. A single Generator drives artifact
        selection, operator choice, argument sampling and Faker, so the same seed reproduces
        the same graph, the same operations and the same artifact contents. Left as None,
        generation is nondeterministic as before.
    :param base_artifact: path to a real seed table (.csv or .parquet) to use as the base
        artifact instead of generating one with Faker. Its schema map is profiled from the
        data. When given, `base_shape` is ignored (a warning is logged, not an error).
        Faker base artifacts have no inter-column correlation, no functional dependencies and
        no semantic column names, so a corpus built only from them teaches an encoder
        features that will not transfer to real tables.
    :param file_format: artifact serialization format, 'csv' or 'parquet'. parquet keeps
        dtypes (csv round-trips everything through text, so an int column comes back as int
        only by luck of inference) and is substantially faster at corpus scale.
    :param operator_policy: 'schema_constrained' (default) picks uniformly among the
        operations the schema permits. 'idiom' samples a latent workflow idiom and biases
        selection toward its next stage, subject to schema legality -- see
        fuzzydata.lineage.idioms. The default makes every edge conditionally independent of
        its predecessors given the schema, so it is itself a test of the state-independence
        assumption rather than a neutral choice; 'idiom' is the correlated alternative.
    :param idiom: name of the idiom to use under operator_policy='idiom'. Omit to sample one.
    :param validate: non-degeneracy check over the finished workflow.
        'warn' (default) logs a summary, 'strict' raises DegenerateArtifactError, 'off'
        skips it. See fuzzydata.lineage.validity.
    :param topology: parent-selection strategy, one of Workflow.TOPOLOGIES
        ('chain'|'star'|'balanced'|'random_recursive'|'bfactor'). Default 'bfactor' keeps the
        historical exponential weighting, in which case `bfactor` applies.
    :return: Workflow object of desired type.
    """
    wf_options = wf_options or {}
    rng = coerce_rng(seed)

    if operator_policy not in ('schema_constrained', 'idiom'):
        raise ValueError(f'Unknown operator_policy {operator_policy!r}; expected '
                         f"'schema_constrained' or 'idiom'")
    idiom_state = None
    if operator_policy == 'idiom':
        from fuzzydata.lineage.idioms import IdiomState, sample_idiom
        idiom_state = IdiomState(idiom or sample_idiom(rng))
        logger.info(f'Workflow {name} follows the {idiom_state.name!r} idiom')

    # Copy the caller's exclusions: this function augments them per-operation below, and
    # mutating the caller's list leaks exclusions across calls.
    user_exclude_ops = set(exclude_ops or [])

    wf = workflow_class(name=name, out_directory=out_directory, file_format=file_format,
                        **wf_options)

    # Ops this client cannot express at all, excluded up front rather than raising mid-run.
    user_exclude_ops |= set(wf.operator_class.unsupported_ops)

    if base_artifact is not None:
        if base_shape != (10, 1000):
            logger.warning(f'base_shape={base_shape} is ignored when base_artifact is given; '
                           f'the seed table determines the base shape.')
        wf.load_base_artifact(load_seed_table(base_artifact))
    else:
        wf.generate_base_artifact(num_cols=base_shape[0], num_rows=base_shape[1], rng=rng)

    num_generated = len(wf.artifact_list)
    artifact_exclusions = []
    stop_generation = False

    while num_generated < num_versions:
        try:
            source_artifact = wf.select_random_artifact(bfactor=bfactor, exclude=artifact_exclusions,
                                                        rng=rng, topology=topology)
            num_ops = 0
            ops_to_do = matfreq  #TODO: Randomize or coin flip here
            force_materialize = False
            did_sibling_split = False
            logger.info(f"Selected Artifact: {source_artifact}, initializing operation chain")
            wf.initialize_operation(artifacts=[source_artifact])

            # if not source_artifact.schema_map:
            #     break
            # current_schema_map = source_artifact.schema_map

            while num_ops < ops_to_do:
                # Do not pivot in the middle of an operation chain. Scoped to this iteration
                # only -- accumulating into the shared list would exclude pivot permanently.
                step_exclude_ops = set(user_exclude_ops)
                if num_ops != ops_to_do-1:
                    step_exclude_ops.add('pivot')

                ops_choices = generate_ops_choices(schema=wf.current_operation.current_schema_map,
                                                   num_rows=len(source_artifact), # TODO: potential num_rows bug
                                                   exclude=step_exclude_ops,
                                                   rng=rng,
                                                   source_df=source_artifact.to_df())

                if ops_choices:
                    logger.debug(f'Ops Choices: {ops_choices}')
                    # Index rather than rng.choice(): the elements are dicts, and numpy
                    # would coerce them into an object array.
                    if idiom_state is not None:
                        selected_op = idiom_state.select(ops_choices, rng)
                    else:
                        selected_op = ops_choices[int(rng.integers(0, len(ops_choices)))]
                    source_artifacts = [source_artifact]

                    if selected_op['op'] == 'train_test_split':
                        # Two destinations from one parent, which the single-destination
                        # Operation model cannot express directly. Emitted as two sibling
                        # operations sharing a random_state and a sibling_group id. The
                        # sides are complementary (train = sampled rows, test = remainder),
                        # so together they partition the parent.
                        if num_generated > num_versions - 2:
                            logger.info('Not enough remaining versions for a train/test '
                                        'split; choosing another operation')
                            continue
                        _generate_sibling_split(wf, source_artifact, selected_op['args'], rng)
                        did_sibling_split = True
                        break

                    # TODO: Handle Merge Op here - materialize/execute before adding right artifact
                    if selected_op['op'] == 'merge':
                        if num_generated == num_versions - 1:
                            logger.warning('Attempting to do merge as last operation; doing another op')
                            continue
                        right_df, right_schema = generate_pkfk_join_table(source_table=source_artifact.to_df(),
                                                                          source_schema=source_artifact.schema_map,
                                                                          key_col=selected_op['args']['key_col'],
                                                                          rng=rng)
                        right_df_label = wf.generate_next_label()
                        right_artifact = wf.initialize_new_artifact(label=right_df_label,
                                                                    filename=wf.artifact_path(right_df_label),
                                                                    schema_map=right_schema)
                        right_artifact.from_df(right_df)
                        wf.add_artifact(right_artifact)
                        wf.current_operation.add_source_artifact(right_artifact)  # TODO: simplify within workflow
                        source_artifacts.append(right_artifact)
                        force_materialize = True

                    try:
                        logger.info(f"Chaining Operation: {selected_op['op']}")
                        wf.chain_to_current_operation([selected_op])
                        # Count the op BEFORE any early break. Previously the
                        # force_materialize break jumped out with num_ops still 0, so the
                        # "if num_ops > 0" guard below skipped materialization entirely: a
                        # merge chosen as the first op in a chain (i.e. always, at matfreq=1)
                        # added its synthesised right-hand table to the workflow and then
                        # never produced the join. The corpus got an orphan parentless
                        # artifact and lost the merge edge.
                        num_ops += 1
                        if force_materialize:
                            break
                    except NotImplementedError as e:
                        logger.warning(f'Attempting an operation that is not implemented for this workflow type:'
                                       f" {selected_op['op']}")
                        raise e
                    except ValueError as e : # Debugging modin-dask groupby x2 error
                        logger.error(f'Source_artifact {source_artifacts[0].label}, Selected Op: {selected_op}')
                        raise e
                    # TODO: Exception Handling for empty datagframes generated
                    # if not next_df:
                    #    logger.warning('Could not apply_op, retrying...')
                else:
                    logger.warning(f"No ops choices available for {source_artifact.label}")
                    artifact_exclusions.append(source_artifact.label)
                    force_materialize = True
                    if set(artifact_exclusions) == set(wf.artifact_list):
                        logger.warning(f"Do not have any options remaining for any of the artifacts.")
                        stop_generation = True
                    break

            # END while num_ops < ops_to_do - we have chained maximum number of ops
            if did_sibling_split:
                pass  # both sides already materialized by _generate_sibling_split
            elif num_ops > 0:
                logger.info(f"Executing current operation list: {wf.current_operation}")
                next_label = wf.generate_next_label()
                wf.execute_current_operation(next_label)
            # TODO: exception handling for failed operation chain
            num_generated = len(wf.artifact_list)
            if stop_generation:
                logger.warning(f'Stopping workflow generation early: Completed {num_generated} artifacts')
                break

        except Exception as e:

            logger.error('Error during generation, stopping...')
            logger.error(f'Was trying to execute operation: {wf.current_operation} on soruce artifact(s): {wf.current_operation.sources}')
            logger.error(f'Was trying to execute code: {wf.current_operation.code}')
            logger.error(f'Writing out all files to {wf.out_dir}')
            wf.serialize_workflow()
            raise e

        # TODO Additional Exception Handling Scenarios
        '''
        except pd.errors.EmptyDataError as e:
            print("Empty DF result")
            retries -= 1
            pass
        except ColumnTypeException as e:
            print(e)
            print("Cannot apply operation because of missing column type, skipping")
            retries -= 1
            pass
        except TooSimilarException as e:
            print(e)
            print("Cannot apply operation because generated dataframe is too similar to ones already generated, skipping")
            retries -= 1
            pass
        except Exception as e:
            dataset.lastargs = {}
            print(dataset.lastmatchoice)
            tb = traceback.format_exc()
            errors.append({choice: tb})
            print(dataset.lastargs)
            raise

        if retries == 0:
            dataset.opcount = 0
            dataset.currentdf = None
            chain_retries -= 1
        '''

    # Record the latent idiom so the corpus can be stratified by it after the fact.
    wf.metadata = {
        'operator_policy': operator_policy,
        'idiom': idiom_state.name if idiom_state else None,
        'seed': seed if isinstance(seed, int) else None,
        'topology': topology,
        'bfactor': bfactor,
        'matfreq': matfreq,
        'file_format': file_format,
        'base_artifact': str(base_artifact) if base_artifact else None,
    }

    wf.serialize_workflow()

    if validate and validate != 'off':
        from fuzzydata.lineage.validity import validate_workflow
        validate_workflow(wf, strict=(validate == 'strict'))

    return wf
