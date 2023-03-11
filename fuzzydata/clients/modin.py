import modin.pandas as mpd
from modin.config import Engine

from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameOperation, DataFrameWorkflow
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


class ModinWorkflow(DataFrameWorkflow):
    def __init__(self, *args, **kwargs):
        self.modin_engine = kwargs.pop('modin_engine', 'dask')
        super(ModinWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = ModinArtifact
        self.operator_class = DataFrameOperation

        self.wf_code_export = self.wf_code_export.replace("import pandas as pd", "import modin.pandas as pd")

        if self.modin_engine == 'dask':
            from dask.distributed import Client
            processes = kwargs.pop('processes', True)
            Client(processes=processes)
            dask_code=f"\nfrom dask.distributed import Client\nClient(processes={processes})"
            self.wf_code_export += dask_code
        else:
            import ray
            ray.init(ignore_reinit_error=True)
            ray_code=f"\nimport ray\nray.init(ignore_reinit_error=True)"
            self.wf_code_export += ray_code
        Engine.put(self.modin_engine)

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return ModinArtifact(label, filename=filename, schema_map=schema_map)
