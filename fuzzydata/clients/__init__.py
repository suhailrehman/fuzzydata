from fuzzydata.clients.modin import ModinWorkflow
from fuzzydata.clients.pandas import DataFrameWorkflow
from fuzzydata.clients.sqlite import SQLWorkflow

supported_workflows = {
    'pandas': DataFrameWorkflow,
    'modin': ModinWorkflow,
    'sql': SQLWorkflow
}


travis_workflows = {
    'pandas': DataFrameWorkflow,
    'sql': SQLWorkflow
}
