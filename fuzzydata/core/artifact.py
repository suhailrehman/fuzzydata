from abc import abstractmethod, ABC

import pandas as pd
import logging

logger = logging.getLogger(__name__)


class Artifact(ABC):
    def __init__(self, label, schema_map=None, filename=None, file_format='csv',
                 in_memory=False, from_df: pd.DataFrame = None):

        self.filename = filename
        self.label = label
        self.in_memory = in_memory
        self.file_format = file_format
        self.schema_map = schema_map

        logger.debug(f'New Artifact: {label}')

    @abstractmethod
    def generate(self, num_rows, schema):
        pass

    @abstractmethod
    def deserialize(self, filename):
        pass

    @abstractmethod
    def serialize(self, filename):
        pass

    @abstractmethod
    def destroy(self):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        pass

    def num_rows(self):
        return len(self.to_df().index)

    def __repr__(self):
        return f"Artifact(label={self.label})"
