import os
import logging
from typing import Dict, List

import networkx as nx
import sqlalchemy

from fuzzydata.core.artifact import Artifact, DataFrameArtifact, SQLArtifact
from fuzzydata.core.generator import generate_schema
from fuzzydata.core.operation import Operation


logger = logging.getLogger(__name__)


class Workflow():
    """
    Class to represent a workflow in fuzzydata, Extends DiGraph from networkx with additional metadata about
    the workflow as required.
    """
    _wf_artifact_mapping = {
        'pandas': DataFrameArtifact,
        'sql': SQLArtifact
    }

    def __init__(self, name='wf', out_directory='/tmp/fuzzydata/wf/', wf_type='pandas'):
        """
        Create a new workflow with a specified name
        :param name: Name of the workflow
        :param out_directory: Output Directory for this workflow
        """


        # if from_dir:
        #     logger.info('Loading Worfklow from {from_dir}', from_dir=from_dir)
        #     graph_file = next(glob(from_dir+"*.gpkl"))
        #     reference_graph = nx.read_gpickle(graph_file)
        #     for node, data in reference_graph.nodes(data=True):
        #         self.add_node(node, **data)
        #     for u,v, data in reference_graph.edges(data=True):
        #         self.add_edge(u, v, **data)

        self.name = name
        self.graph = nx.DiGraph()
        self.out_dir = out_directory
        self.artifact_dir = f"{self.out_dir}/artifacts/"
        os.makedirs(self.artifact_dir, exist_ok=True)
        self.artifact_list = []
        self.artifact_dict = {}
        self.artifact_class = self._wf_artifact_mapping[wf_type]
        logger.info(f'Creating new Workflow {self.name}')

        # Hack for SQL-based workflows - refactor for cleaner interface.
        if wf_type == 'sql':
            self.sql_engine = sqlalchemy.create_engine(f"sqlite:///{self.out_dir}/{self.name}.db")

    def generate_next_label(self):
        return f"artifact_{len(self)}"

    def initialize_new_artifact(self, label=None, filename=None):
        if not filename:
            filename = f"{self.artifact_dir}/{label}.csv"
        if not label:
            label = self.generate_next_label()
        if self.artifact_class == SQLArtifact:
            return SQLArtifact(label, filename=filename, sql_engine=self.sql_engine)
        else:
            return DataFrameArtifact(label, filename=filename)

    def add_artifact(self, artifact: Artifact, from_artifacts: List[Artifact] = [], operation: Operation = None) -> None:
        """
        Add a new artifact to the workflow with label and dataframe.
        Optionally add source label and edge information
        """
        self.graph.add_node(artifact.label,
                            **{
                                'schema_map': artifact.schema_map,
                                'file_format': artifact.file_format,
                                'filename' : artifact.filename
                            })
        v = artifact.label

        self.artifact_list.append(artifact.label)
        self.artifact_dict[artifact.label] = artifact

        for u in from_artifacts:
            self.graph.add_edge(u, v, **{
                'operation': operation.op,
                'args': operation.args
            })

    def generate_base_artifact(self, num_rows=100, num_cols=10, column_maps=None, label: str = None) -> None:
        """
        Create a base artifact of with given rows and columns
        :param num_rows:
        :param num_cols:
        :param column_maps:
        :param label:
        :return:
        """
        if not column_maps:
            column_maps = generate_schema(num_cols)

        new_artifact = self.initialize_new_artifact(label=label)
        new_artifact.generate(num_rows, column_maps)
        self.add_artifact(new_artifact)

    def generate_artifact_from_operation(self, artifacts: List[Artifact], op: str, args: Dict) -> None:
        """
        :param args:
        :param artifacts:
        :param op:
        :return:
        """
        operation = Operation(sources=artifacts, new_label=self.generate_next_label(), op=op, args=args)
        new_artifact = operation.execute()
        self.add_artifact(new_artifact, from_artifacts=artifacts, operation=operation)

    def __len__(self):
        return len(self.artifact_list)

    def __getitem__(self, item):
        return self.artifact_dict[item]



