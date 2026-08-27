# -*- coding: utf-8 -*-

"""
fuzzydata.core.operation
~~~~~~~~~~~~
This module contains the abstract implementation of an operation in fuzzydata
:copyright: (c) Suhail Rehman 2022
:license: MIT, see LICENSE for more details.
"""

import logging
import time
from abc import ABC, abstractmethod
from typing import List, TypeVar, Generic, Dict

from fuzzydata.core.artifact import Artifact

T = TypeVar('T')

logger = logging.getLogger(__name__)


class Operation(Generic[T], ABC):

    #: Operations this client cannot express. Declared per-client so the generator can
    #: exclude them up front instead of discovering them via NotImplementedError mid-run.
    unsupported_ops = frozenset()

    def __init__(self, sources: List[Artifact]):
        """Initialize a new operation with a list of source artifacts
        :param sources: List of source artifacts for this operation.
        """
        self.sources = sources
        self.new_label = None
        self.dest_schema_map = None

        # Operation Timings
        self.start_time = None
        self.end_time = None

        # Code Generation Variables
        self.code = ''
        # Copy, do not alias. apply() inserts a key into current_schema_map, and without a
        # copy that mutated the SOURCE artifact's schema_map in place -- the parent then
        # advertised a derived column it does not contain, and any later operation reading
        # that schema emitted code referencing a nonexistent column. Every other operator
        # rebuilds the dict, so apply was the only one that could trigger it, which is why
        # this stayed hidden while apply was commented out of the generator.
        self.current_schema_map = dict(self.sources[0].schema_map or {})
        self.num_operations = 0
        self.op_list = []  # List[Dict] of op names and args to chain together.

        #: Shared id linking operations that were co-generated from one parent and together
        #: partition it (currently only train_test_split). None for ordinary operations.
        #: Recorded because the relationship is impossible to recover after generation.
        self.sibling_group = None

    def add_source_artifact(self, s_artifact: Artifact) -> None:
        """Add a source artifact to this operation. """
        self.sources.append(s_artifact)

    def resolve_schema_map(self, materialized_df) -> Dict[str, str]:
        """Schema map to record on the artifact this operation produced.

        pivot() deliberately blanks current_schema_map because the destination columns are
        data-dependent and only known once the operation runs. Nothing used to refill it, so
        every post-pivot artifact was recorded with an empty schema -- which made it a
        permanent dead end for further generation and left a hole in the ground truth the
        corpus depends on. Profile the actual result instead.
        """
        if self.current_schema_map:
            return self.current_schema_map
        from fuzzydata.lineage.profiler import infer_schema_map
        if materialized_df is None:
            return {}
        resolved = infer_schema_map(materialized_df)
        logger.debug(f'Recovered schema map for {self.new_label} by profiling: {resolved}')
        self.current_schema_map = resolved
        return resolved

    @staticmethod
    def one_hot_column_name(column: str, category) -> str:
        """Name of an indicator column produced by one_hot_encode. Defined once so the
        schema-map transformation and every client's emitted code agree."""
        return f'{column}__is_{category}'

    @staticmethod
    def apply_column_name(numeric_col: str, a, b) -> str:
        """Name of the column that apply() derives. Defined once here because the ABC's
        schema-map update and every client's emitted code must agree exactly. They did not:
        the ABC and the sql client used {a}x_{b} while the pandas client used
        int(a)x_int(b), so for non-integer scalars the schema map recorded a column name
        that did not exist in the materialized dataframe.

        The result must be a valid Python identifier: the pandas client emits
        `.assign(<name> = lambda ...)` and that string is eval()'d, so a float scalar like
        0.5 cannot appear verbatim.
        """
        def _fmt(v):
            return f"{v}".replace('-', 'neg').replace('.', 'p')
        return f"{numeric_col}__{_fmt(a)}x_{_fmt(b)}"

    @abstractmethod
    def sample(self, frac: float, random_state: int = None) -> T:
        """ Sample frac proportion of rows from the source artifact
        :param frac: fraction [0.0,1.0] of rows to sample from the artifact
        :param random_state: concrete seed for the draw. Emitted into the generated code and
            persisted in the operation record, so replay reproduces the same rows. Without
            it a serialized workflow advertised a replayability it did not have.
        :return:
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def apply(self, numeric_col: str, a: float, b: float) -> T:
        """ Apply a linear (ax+b) transformation for every element x in numeric_col
        :param numeric_col: The label of the column to the transformed
        :param a: scale
        :param b: offset
        :return:
        """
        self.current_schema_map[self.apply_column_name(numeric_col, a, b)] = \
            self.current_schema_map[numeric_col]
        pass

    @abstractmethod
    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> T:
        """
        Groupby group_columns and apply agg_function to the agg_columns
        :param group_columns: The columns to group on
        :param agg_columns: The columns to apply aggregate function on
        :param agg_function: The aggregation function to be applied (min,max,mean,count)
        :return:
        """
        output_cols = list(group_columns) + list(agg_columns)
        self.current_schema_map = dict(filter(lambda x: x[0] in output_cols, self.current_schema_map.items()))
        pass

    @abstractmethod
    def project(self, output_cols: List[str]) -> T:
        """
        Project only output_cols
        :param output_cols: The column labels to be projected
        :return:
        """
        self.current_schema_map = dict(filter(lambda x: x[0] in output_cols, self.current_schema_map.items()))
        pass

    @abstractmethod
    def select(self, condition: str) -> T:
        """
        Select rows satisfying a specific condition for the column
        :param condition: string condition `column_name` >=< `value`
        :return:
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def merge(self, key_col: List[str]) -> T:
        """
        Merge the source artifacts defined in this operation on key_column
        :param key_col: The common column to be used for the merge.
        :return:
        """
        self.current_schema_map = {**self.current_schema_map, **self.sources[1].schema_map}
        pass

    @abstractmethod
    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        """
        Pivot the dataframe with new index `index_cols, on `columns` using `value_col` with agg_func
        :param index_cols: The column label to be used for the index
        :param columns: The column label to be used for the new columns
        :param value_col: The column label to be used for the values in the pivoted table
        :param agg_func: Aggregation function to be used in case of multiple index,column pairs
        :return:
        """
        # Destination Schema Map should be generated by operation!
        # TODO: prevent further op generation after this point since we MUST materialize.
        self.current_schema_map = {}
        pass

    @abstractmethod
    def fill(self, col_name: str, old_value, new_value) -> T:
        """
        Fill a dataframe, changing "old_value" to "new_value" in the column with label "col_name"
        :param col_name: Label of the column to be filled
        :param old_value: Value to be replaced
        :param new_value: New Value
        :return:
        """
        self.current_schema_map = self.current_schema_map
        pass

    # ------------------------------------------------------------------ 0.1.0 operators
    # The set above is BI-shaped. The ML-training-data workflows this corpus represents are
    # dominated by the operators below. Each declares its schema-map transformation here;
    # clients emit the code.

    @abstractmethod
    def dropna(self, subset: List[str] = None) -> T:
        """Drop rows with nulls, optionally restricted to `subset`.
        Cardinality-reducing, schema-preserving, not invertible.
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def dedupe(self, subset: List[str] = None) -> T:
        """Drop duplicate rows, optionally keyed on `subset`.
        Cardinality-reducing and schema-preserving. Note it can *increase* the empirical
        entropy of the row distribution by removing over-represented rows.
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def rename(self, column_map: Dict[str, str]) -> T:
        """Rename columns. Metadata-only, so losslessly invertible -- which is why it matters
        for the equivalence classes in fuzzydata.lineage.equivalence.
        :param column_map: {old_name: new_name}
        """
        self.current_schema_map = {column_map.get(k, k): v
                                   for k, v in self.current_schema_map.items()}
        pass

    @abstractmethod
    def astype(self, column: str, dtype: str) -> T:
        """Coerce `column` to `dtype`. Schema-preserving in shape; lossless only for
        widening casts, which is what decides invertibility.

        As with label_encode, the recorded provider has to follow the actual type.
        """
        from fuzzydata.lineage.profiler import profiled_provider
        import numpy as np
        try:
            kind = np.dtype(dtype).kind
        except TypeError:
            kind = 'O'
        label = 'numeric' if kind in 'ifb' else 'string'
        if column in self.current_schema_map:
            self.current_schema_map = {**self.current_schema_map,
                                       column: profiled_provider(label)}
        pass

    @abstractmethod
    def normalize(self, column: str) -> T:
        """Min-max scale `column` into [0, 1], in place. Invertible in principle but the
        constants are not recorded, so treated as lossy here.
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def standardize(self, column: str) -> T:
        """Zero-mean, unit-variance scale `column`, in place."""
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def label_encode(self, column: str) -> T:
        """Replace a categorical column's values with integer codes, in place.

        The column's TYPE changes, so the schema map must change with it. Leaving the
        original provider in place made the schema lie about the data: a later fill() would
        draw an integer code as old_value and fake a same-provider string as new_value,
        producing a mixed int/str column that parquet refuses to write.
        """
        from fuzzydata.lineage.profiler import profiled_provider
        if column in self.current_schema_map:
            self.current_schema_map = {**self.current_schema_map,
                                       column: profiled_provider('numeric')}
        pass

    @abstractmethod
    def one_hot_encode(self, column: str, categories: List[str]) -> T:
        """Expand a categorical column into one indicator column per category.

        Column-count explosion -- this is the operator that stresses schema tracking hardest.
        `categories` is recorded explicitly rather than inferred at run time, because the
        destination schema has to be known without executing the operation, and inferring it
        from data would make replay depend on the data rather than the spec.
        """
        # Indicator columns are 0/1 integers, not values of the source provider. Recording
        # the source provider would misdescribe them the same way label_encode did.
        from fuzzydata.lineage.profiler import profiled_provider
        indicator_provider = profiled_provider('numeric')
        self.current_schema_map = {k: v for k, v in self.current_schema_map.items()
                                   if k != column}
        for category in categories:
            self.current_schema_map[self.one_hot_column_name(column, category)] = indicator_provider
        pass

    @abstractmethod
    def train_test_split(self, frac: float, random_state: int, side: str) -> T:
        """One side of a train/test split.

        Modelled as two sibling operations sharing `random_state`, linked by a sibling_group
        id, because Operation is single-destination (making it multi-destination is Track B).
        The two sides are COMPLEMENTARY, not two independent draws: `side='train'` takes the
        sampled rows and `side='test'` takes the remainder. Sharing a seed alone would give
        two overlapping samples mislabelled as a partition.
        :param frac: fraction of rows in the train side.
        :param random_state: shared seed; identical for both siblings.
        :param side: 'train' or 'test'.
        """
        self.current_schema_map = self.current_schema_map
        pass

    @abstractmethod
    def chain_operation(self, op, args):
        """
        Chain an operation to the list of operations to be materialized.
        :param op: string label of operation to chain
        :param args: arguments of the operation to chain
        :return: None
        """
        """ Take the current operation information stored and generate the code to be chained. 
            Assume self.code is updated here by the client implementation, so basically, this just updates
            the op_list metadata. client implementation super() class should be AFTER code is generated. """
        self.op_list.append({'op': op, 'args': args})
        self.num_operations += 1


    @abstractmethod
    def materialize(self, new_label) -> T:
        """
        Execute all stacked/chained operations and generate a new artifact with label "new_label"
        :param new_label: The new label of the artifact to be produced.
        :return:
        """
        self.new_label = new_label

    def execute(self, new_label) -> T:
        """
        Execute all stacked/chained operations and generate a new artifact with label "new_label"
        Add performance information to the operation object.
        :param new_label: The new label of the artifact to be produced.
        :return: The new artifact that is produced.
        """
        logger.debug(f"Before Op: {self.sources[0].to_df().columns}")
        logger.debug(f"Operation Code: {self.code}")
        self.start_time = time.perf_counter()
        result = self.materialize(new_label)
        self.end_time = time.perf_counter()
        return result

    #: Operations that are never invertible: information is destroyed and cannot be
    #: reconstructed from the output alone.
    NON_INVERTIBLE_OPS = frozenset({
        'project',            # dropped columns are gone
        'select', 'dropna', 'dedupe',   # dropped rows are gone
        'sample', 'train_test_split',   # dropped rows are gone
        'groupby',            # many rows collapse to one
        'pivot',              # layout change plus aggregation
        'normalize', 'standardize',     # min/max/mean/std are not recorded
        'label_encode',       # the value -> code mapping is not recorded
        'merge',              # the added columns have no inverse on the left input
        # one_hot_encode is information-preserving when every row carries exactly one of
        # the recorded categories, but nulls and unseen values are silently dropped and we
        # do not record whether that happened. Treated as non-invertible on purpose:
        # over-claiming invertibility merges artifacts that are not actually equivalent,
        # which is a worse error for the quotient-aware metrics than splitting too finely.
        'one_hot_encode',
    })

    #: Casts that lose nothing, as (from_kind, to_kind) numpy dtype kinds.
    LOSSLESS_CASTS = frozenset({('i', 'f'), ('i', 'i'), ('f', 'f'), ('b', 'i'), ('b', 'f')})

    def is_invertible_on(self, artifact=None) -> bool:
        """Whether this operation's whole chain can be inverted on `artifact`.

        A chain is invertible only if every step is. `artifact` is the input the operation
        was applied to; some rules need its data (see fill and astype below), and if it is
        not supplied those rules answer conservatively.

        Invertibility is what defines the equivalence classes in
        fuzzydata.lineage.equivalence: lineage is identifiable only up to mutual
        derivability, so two artifacts joined by invertible edges are the same object for
        the purpose of the quotient-aware metrics.
        """
        if not self.op_list:
            return False
        return all(self._step_is_invertible(entry['op'], entry['args'], artifact)
                   for entry in self.op_list)

    def is_invertible_on_input(self) -> bool:
        """is_invertible_on() against this operation's own first source."""
        source = self.sources[0] if self.sources else None
        return self.is_invertible_on(source)

    def _step_is_invertible(self, op: str, args: Dict, artifact) -> bool:
        if op in self.NON_INVERTIBLE_OPS:
            return False

        if op == 'rename':
            # Metadata only. Losslessly invertible by swapping the mapping.
            return True

        if op == 'apply':
            # y = ax + b recovers x iff a != 0. The source column is kept alongside the
            # derived one, so even a == 0 loses nothing -- but the derived column itself is
            # then constant, and the operation is not information-adding. Follow the spec.
            return args.get('a', 0) != 0

        if op == 'astype':
            return self._cast_is_lossless(args.get('column'), args.get('dtype'), artifact)

        if op == 'fill':
            return self._fill_is_invertible(args, artifact)

        # Unknown operator: assume not invertible rather than silently claiming equivalence.
        logger.warning(f'No invertibility rule for operation {op!r}; assuming not '
                       f'invertible. Add it to Operation._step_is_invertible.')
        return False

    def _cast_is_lossless(self, column, dtype, artifact) -> bool:
        """A cast is invertible only if it widens. int -> float round-trips; float -> int
        truncates; anything involving text is lossy."""
        if artifact is None or column is None:
            return False
        try:
            import numpy as np
            source_kind = np.dtype(artifact.to_df()[column].dtype).kind
            target_kind = np.dtype(dtype).kind
        except (KeyError, TypeError, ValueError, AttributeError):
            return False
        return (source_kind, target_kind) in self.LOSSLESS_CASTS

    def _fill_is_invertible(self, args, artifact) -> bool:
        """fill is invertible iff the replacement value did NOT already occur in the column.

        If it did, the output has two indistinguishable populations -- rows that always held
        new_value and rows that were changed to it -- and the original cannot be recovered.
        The spec says to compute this rather than guess, so it is computed.
        """
        if artifact is None:
            return False
        column, new_value = args.get('col_name'), args.get('new_value')
        try:
            series = artifact.to_df()[column]
        except (KeyError, TypeError, AttributeError):
            return False
        return not bool((series == new_value).any())

    def get_execution_time(self):
        """ Get the execution time for this operation"""
        return self.end_time - self.start_time

    def to_dict(self) -> dict:
        """ Return a dictionary representation of this operation"""
        from fuzzydata.lineage.annotations import annotate_edge
        record = {
            'sources': [s.label for s in self.sources],
            'new_label': self.new_label,
            'op_list': self.op_list,
        }
        if self.sibling_group is not None:
            record['sibling_group'] = self.sibling_group
        # Per-edge provenance metadata. Free to emit here and effectively unrecoverable once
        # the artifacts are on disk. See fuzzydata.lineage.annotations.
        record['annotation'] = annotate_edge(self)
        return record

    def __str__(self):
        return str(self.to_dict())
