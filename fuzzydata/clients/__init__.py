import importlib
from copy import deepcopy

from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.clients.sqlite import SQLWorkflow

travis_workflows = {
    'pandas': DataFrameWorkflow,
    'sql': SQLWorkflow
}

supported_workflows = deepcopy(travis_workflows)

modin_spec = importlib.util.find_spec('modin')
if modin_spec:
    from fuzzydata.clients.modin import ModinWorkflow
    supported_workflows['modin'] = ModinWorkflow
