import math
from abc import ABC, abstractmethod
from typing import List, TypeVar, Generic

from fuzzydata.core.artifact import Artifact, DataFrameArtifact, SQLArtifact

T = TypeVar('T')


class Operation(Generic[T], ABC):
    def __new__(cls, sources: List[Artifact], new_label: str):
        first_artifact = sources[0]
        subclass = None
        if isinstance(first_artifact, DataFrameArtifact):
            subclass = DataFrameOperation
        elif isinstance(first_artifact, SQLArtifact):
            subclass = SQLOperation
        instance = super(Operation, subclass).__new__(subclass)
        # instance.__init__(sources=sources, new_label=new_label)
        return instance

    def __init__(self, sources: List[Artifact], new_label: str):
        self.sources = sources
        self.new_label = new_label
        self.dest_schema_map = None

    @abstractmethod
    def sample(self, frac: float) -> T:
        self.dest_schema_map = self.sources[0].schema_map
        pass

    # @abstractmethod
    # def groupby(self, sources: Artifact, columns: List[str], agg_func: str) -> Artifact:
    #     pass


class DataFrameOperation(Operation['DataFrameArtifact']):

    def __init__(self, *args, **kwargs):
        super(DataFrameOperation, self).__init__(*args, **kwargs)

    def sample(self, frac: float) -> DataFrameArtifact:
        super(DataFrameOperation, self).sample(frac)
        return DataFrameArtifact(label=self.new_label,
                                 from_df=self.sources[0].table.sample(frac=frac),
                                 schema_map=self.dest_schema_map)


class SQLOperation(Operation['SQLArtifact']):

    def __init__(self, *args, **kwargs):
        super(SQLOperation, self).__init__(*args, **kwargs)

    def sample(self, frac: float) -> SQLArtifact:
        super(SQLOperation, self).sample(frac)
        num_rows = len(self.sources[0])
        sample_rows = math.ceil(num_rows*frac)
        sql_sample_stmt = f"CREATE TABLE {self.new_label} AS SELECT * FROM {self.sources[0].label} ORDER BY RANDOM() " \
                          f"LIMIT {sample_rows} "
        return SQLArtifact(label=self.new_label,
                           sql_engine=self.sources[0].sql_engine,
                           from_sql=sql_sample_stmt,
                           schema_map=self.dest_schema_map)
