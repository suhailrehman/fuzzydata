import modin.pandas as mpd
from modin.config import Engine

from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameOperation
from fuzzydata.core.workflow import Workflow


class ModinArtifact(DataFrameArtifact):

    def __init__(self, *args, **kwargs):
        kwargs.update({'pd': mpd})  # Force loading of the modin pandas library
        super(ModinArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'csv': self.pd.read_csv
        }
        self._serialization_function = {
            'csv': 'to_csv'
        }

        self.operation_class = DataFrameOperation


class ModinWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        self.modin_engine = kwargs.pop('modin_engine', 'dask')
        super(ModinWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = ModinArtifact
        self.operator_class = DataFrameOperation

        if self.modin_engine == 'dask':
            from dask.distributed import Client
            Client()
        else:
            import ray
            ray.init(ignore_reinit_error=True)

        Engine.put(self.modin_engine)

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return ModinArtifact(label, filename=filename, schema_map=schema_map)
