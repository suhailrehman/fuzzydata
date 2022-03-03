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
        """ Abstract method which invokes generate_table function and stores it somehow  """

    @abstractmethod
    def from_df(self, df):
        """ Abstract method which accepts a dataframe as input and stores inside this artifact object"""

    @abstractmethod
    def deserialize(self, filename):
        """ Abstract method to load artifact from disk using some serialization method"""

    @abstractmethod
    def serialize(self, filename):
        """ Abstract method to store artifact to disk using some serialization method"""

    @abstractmethod
    def destroy(self):
        """ Destructor when this artifact needs to deleted from memory"""

    @abstractmethod
    def to_df(self) -> pd.DataFrame:
        """ Return a dataframe representation of this artifact"""

    def __len__(self):
        """ Abstract representation: should return the number of rows in this artifact"""

    def __repr__(self):
        return f"Artifact(label={self.label})"
