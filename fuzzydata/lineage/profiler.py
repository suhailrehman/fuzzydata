# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.profiler
~~~~~~~~~~~~

Profile a real DataFrame into fuzzydata's internal column-type labels, so a workflow can be
generated from a real seed table instead of Faker output.

Why this matters for a lineage corpus: Faker base artifacts have no inter-column correlation,
no functional dependencies and no semantic column names. An encoder trained only on them
learns features that will not transfer to real tables.

fuzzydata's generator reasons about four labels -- numeric, groupable, joinable, string --
which it derives from a schema_map of {column: faker_provider}. A real table has no Faker
providers, so we emit sentinel provider names of the form "__profiled_<label>" that map back
to the same four labels via the same inverse-lookup the generator already uses.
"""

import logging
from typing import Dict, List

import pandas as pd

logger = logging.getLogger(__name__)

#: The four column-type labels the generator reasons about.
COLUMN_TYPES = ('numeric', 'groupable', 'joinable', 'string')

#: Sentinel provider prefix for profiled (non-Faker) columns.
PROFILED_PREFIX = '__profiled_'

#: A column is groupable when its distinct-value ratio is at or below this.
GROUPABLE_MAX_CARDINALITY_RATIO = 0.5

#: A column is joinable when its distinct-value ratio is at or above this...
JOINABLE_MIN_UNIQUENESS = 0.95
#: ...and its null rate is at or below this.
JOINABLE_MAX_NULL_RATE = 0.05


class InsufficientSchemaError(ValueError):
    """Raised when a profiled table admits no legal operations.

    Carries the missing label so the caller learns *why* the table is unusable, instead of
    failing deep inside the generator with an unrelated KeyError.
    """

    def __init__(self, message: str, missing: List[str] = None):
        super().__init__(message)
        self.missing = list(missing or [])


def profiled_provider(label: str) -> str:
    """Sentinel provider name standing in for a Faker provider on a profiled column."""
    return f'{PROFILED_PREFIX}{label}'


def is_profiled_provider(provider: str) -> bool:
    return isinstance(provider, str) and provider.startswith(PROFILED_PREFIX)


def _looks_like_identifier(name: str, series: pd.Series) -> bool:
    """Integer-valued columns that are near-unique and named like a key are identifiers, not
    quantities. Treating an id as numeric invites meaningless arithmetic (apply) and
    meaningless aggregation (groupby)."""
    if not pd.api.types.is_integer_dtype(series):
        return False
    lowered = str(name).lower()
    if lowered.endswith(('id', '_id', 'key', '_key', 'no', 'num')) or lowered in ('id', 'index'):
        return True
    return _uniqueness(series) >= JOINABLE_MIN_UNIQUENESS


def _uniqueness(series: pd.Series) -> float:
    non_null = series.dropna()
    if not len(non_null):
        return 0.0
    return non_null.nunique() / len(non_null)


def infer_column_labels(name: str, series: pd.Series) -> List[str]:
    """Labels applicable to one column. A column may carry several, as Faker columns do."""
    labels = []
    non_null = series.dropna()
    if not len(non_null):
        return ['string']  # entirely null: only safe as an opaque passenger column

    uniqueness = _uniqueness(series)
    null_rate = 1.0 - (len(non_null) / len(series))

    numeric = pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)
    if numeric and not _looks_like_identifier(name, series):
        labels.append('numeric')

    if uniqueness <= GROUPABLE_MAX_CARDINALITY_RATIO:
        labels.append('groupable')

    if uniqueness >= JOINABLE_MIN_UNIQUENESS and null_rate <= JOINABLE_MAX_NULL_RATE:
        labels.append('joinable')

    if pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series):
        labels.append('string')

    if not labels:
        # Numeric-but-identifier-like, or high-cardinality non-string: still joinable-ish.
        labels.append('joinable' if uniqueness >= JOINABLE_MIN_UNIQUENESS else 'string')

    return labels


def infer_schema_map(df: pd.DataFrame) -> Dict[str, str]:
    """Profile a real DataFrame into a fuzzydata schema_map.

    :param df: the real table to profile.
    :return: {column_name: sentinel_provider}. One provider per column, matching the
        {column: provider} shape the rest of fuzzydata expects. Where a column qualifies for
        several labels the most specific is chosen, and the full label set is still
        recoverable via infer_column_types().
    """
    schema_map = {}
    for name in df.columns:
        labels = infer_column_labels(name, df[name])
        # Preference order: numeric carries the most generative power (enables apply,
        # groupby aggregation and pivot values), then joinable, then groupable, then string.
        for preferred in ('numeric', 'joinable', 'groupable', 'string'):
            if preferred in labels:
                schema_map[str(name)] = profiled_provider(preferred)
                break
    logger.debug(f'Profiled schema map: {schema_map}')
    return schema_map


def infer_column_types(df: pd.DataFrame) -> Dict[str, List[str]]:
    """Full label sets per column, i.e. {column: [label, ...]}. Useful for diagnostics and
    for validating that a table admits any operations at all."""
    return {str(name): infer_column_labels(name, df[name]) for name in df.columns}


def validate_schema_map(schema_map: Dict[str, str], df: pd.DataFrame = None) -> None:
    """Fail fast if a profiled table cannot support any operation.

    The generator needs a numeric column for apply/groupby/pivot/select, and needs at least
    three columns before project is offered. A table of unique strings admits nothing.
    """
    from fuzzydata.core.generator import get_schema_type_mapping

    present = set(get_schema_type_mapping(schema_map).keys())
    if 'numeric' not in present:
        raise InsufficientSchemaError(
            'Profiled table has no usable numeric column, so no apply, groupby, pivot or '
            'select operation can be generated from it. Provide a seed table with at least '
            'one non-identifier numeric column.',
            missing=['numeric'])
    if len(schema_map) < 2:
        raise InsufficientSchemaError(
            f'Profiled table has only {len(schema_map)} column(s); at least 2 are needed to '
            'generate any operation.', missing=['columns'])
