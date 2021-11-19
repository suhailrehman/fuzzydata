import pandas as pd
from loguru import logger


_deserialization_function = {
    'csv': pd.read_csv
}

_serialization_function = {
    'csv': 'to_csv'
}


class Artifact:
    def __init__(self, label, schema_map=None, filename=None, file_format='csv',
                 in_memory=True, df=None, generate=False):
        if generate:
            # TODO: Call df generation function, write to disk and store filename here.
            # Check and generate with schema map or call a new function to generate the schema map from scratch.
            pass
        if not filename:
            filename = f"{label}.csv"

        self.filename = filename
        self.label = label
        self.in_memory = in_memory
        self.format = file_format
        self.schema_map = schema_map

        logger.info('New Artifact: {label}', label=label)

        if in_memory:
            # Either load the artifact from filename or copy from df parameter
            if df:
                self.table = df
            else:
                self.load_df()

    def load_df(self):
        self.table = _deserialization_function[self.file_format](self.filename)
        self.in_memory = True

    def store_df(self):
        if self.in_memory:
            self.df.get_attr(_serialization_function[self.file_format])(self.filename)
