# -*- coding: utf-8 -*-

"""
fuzzydata.clients.modin
~~~~~~~~~~~~

DEPRECATED as of 0.1.0, and not covered by CI.

This client is a thin subclass of the pandas client -- both ModinArtifact and ModinWorkflow
reuse DataFrameOperation verbatim, so all transformation and code-generation logic is
exercised by the pandas tests. What is *not* exercised is this module's own glue, because
ModinWorkflow.__init__ starts a dask.distributed cluster (or a ray runtime) on construction,
which is too heavy and too flaky to run in CI.

The client still ships and still works; it is simply unsupported. To exercise it:

    pip install 'fuzzydata[modin]'
    FUZZYDATA_TEST_MODIN=1 pytest

:copyright: (c) Suhail Rehman 2022
:license: MIT, see LICENSE for more details.
"""

import warnings

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
        warnings.warn(
            'The fuzzydata modin client is deprecated as of 0.1.0 and is not covered by '
            'CI. It still works, but is unsupported; prefer the pandas or sql clients.',
            DeprecationWarning,
            stacklevel=2,
        )
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
