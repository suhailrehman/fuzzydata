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
        self.current_schema_map = self.sources[0].schema_map
        self.num_operations = 0
        self.op_list = []  # List[Dict] of op names and args to chain together.

    def add_source_artifact(self, s_artifact: Artifact) -> None:
        """Add a source artifact to this operation. """
        self.sources.append(s_artifact)

    @abstractmethod
    def sample(self, frac: float) -> T:
        """ Sample frac proportion of rows from the source artifact
        :param frac: fraction [0.0,1.0] of rows to sample from the artifact
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
        self.current_schema_map = self.current_schema_map
        self.current_schema_map[f"{numeric_col}__{a}x_{b}"] = self.current_schema_map[numeric_col]
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

    def get_execution_time(self):
        """ Get the execution time for this operation"""
        return self.end_time - self.start_time

    def to_dict(self) -> dict:
        """ Return a dictionary representation of this operation"""
        return {
            'sources': [s.label for s in self.sources],
            'new_label': self.new_label,
            'op_list': self.op_list,
        }

    def __str__(self):
        return str(self.to_dict())
