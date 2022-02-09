import logging
from typing import List

import pandas

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T
from fuzzydata.core.workflow import Workflow

logger = logging.getLogger(__name__)


class DataFrameArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.pd = kwargs.pop("pd", pandas)
        from_df = kwargs.pop("from_df", None)
        super(DataFrameArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'csv': self.pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

        self.operation_class = DataFrameOperation

        if from_df is not None:
            self.table = self.pd.DataFrame(from_df)
            self.in_memory = True

    def generate(self, num_rows, schema):
        self.table = generate_table(num_rows, column_dict=schema, pd=self.pd)
        self.schema_map = schema
        self.in_memory = True

    def deserialize(self, filename=None):
        if not filename:
            filename = self.filename

        self.table = self._deserialization_function[self.file_format](filename)
        self.in_memory = True

    def serialize(self, filename=None):
        if not filename:
            filename = self.filename

        if self.in_memory:
            serialization_method = getattr(self.table, self._serialization_function[self.file_format])
            serialization_method(filename)

    def destroy(self):
        del self.table

    def to_df(self) -> pandas.DataFrame:
        return self.table

    def __len__(self):
        if self.in_memory:
            return len(self.table.index)


class DataFrameOperation(Operation['DataFrameArtifact']):
    def __init__(self, *args, **kwargs):
        self.artifact_class = kwargs.pop('artifact_class', DataFrameArtifact)
        super(DataFrameOperation, self).__init__(*args, **kwargs)

    def sample(self, frac: float) -> DataFrameArtifact:
        super(DataFrameOperation, self).sample(frac)
        return self.artifact_class(label=self.new_label,
                                 from_df=self.sources[0].table.sample(frac=frac),
                                 schema_map=self.dest_schema_map)

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> T:
        super(DataFrameOperation, self).groupby(group_columns, agg_columns, agg_function)
        logger.debug(f"Groupby on {self.sources[0].label} : {group_columns}/{agg_columns}")
        project_df = self.sources[0].table[group_columns + agg_columns]
        groupby_instance = getattr(project_df.groupby(group_columns),agg_function)
        new_df = groupby_instance().reset_index(drop=False)
        return self.artifact_class(label=self.new_label,
                                 from_df=new_df,
                                 schema_map=self.dest_schema_map)

    def project(self, output_cols: List[str]) -> T:
        super(DataFrameOperation, self).project(output_cols)
        return self.artifact_class(label=self.new_label,
                                 from_df=self.sources[0].table[output_cols].copy(),
                                 schema_map=self.dest_schema_map)

    def select(self, condition: str) -> T:
        super(DataFrameOperation, self).select(condition)
        return self.artifact_class(label=self.new_label,
                                 from_df=self.sources[0].table.query(condition),
                                 schema_map=self.dest_schema_map)

    def merge(self, key_col: List[str]) -> T:
        super(DataFrameOperation, self).merge(key_col)
        merge_result = self.sources[0].table.merge(self.sources[1].table, on=key_col)
        return self.artifact_class(label=self.new_label,
                                 from_df=merge_result,
                                 schema_map=self.dest_schema_map)

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        super(DataFrameOperation, self).pivot(index_cols, columns, value_col, agg_func)
        pivot_result = self.sources[0].table.pivot_table(index=index_cols, columns=columns,
                                                         values=value_col, aggfunc=agg_func)
        return self.artifact_class(label=self.new_label,
                                 from_df=pivot_result,
                                 schema_map=self.dest_schema_map)


class DataFrameWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super(DataFrameWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = DataFrameArtifact
        self.operator_class = DataFrameOperation

    def initialize_new_artifact(self, label=None, filename=None):
        return DataFrameArtifact(label, filename=filename)
