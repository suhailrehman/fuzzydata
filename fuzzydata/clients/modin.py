import modin

from fuzzydata.clients.pandas import DataFrameArtifact, DataFrameOperation
from fuzzydata.core.workflow import Workflow


class ModinArtifact(DataFrameArtifact):

    def __init__(self, *args, **kwargs):
        kwargs.update({'pd': modin.pandas})
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
        super(ModinWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = ModinArtifact
        self.operator_class = DataFrameOperation

    def initialize_new_artifact(self, label=None, filename=None):
        return ModinArtifact(label, filename=filename)
