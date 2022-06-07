from __future__ import annotations

import glob
import os
import logging
import json
import time

from abc import ABC, abstractmethod
from typing import Dict, List

import networkx as nx
import numpy as np
import pandas as pd

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_schema
from fuzzydata.core.operation import Operation


logger = logging.getLogger(__name__)


class Workflow(ABC):
    """
    Class to represent a workflow in fuzzydata, Extends DiGraph from networkx with additional metadata about
    the workflow as required.
    """
    def __init__(self, name='wf', out_directory='/tmp/fuzzydata/wf/'):
        """
        Create a new workflow with a specified name
        :param name: Name of the workflow
        :param out_directory: Output Directory for this workflow
        """

        self.name = name
        self.graph = nx.DiGraph()
        self.out_dir = out_directory
        self.artifact_dir = f"{self.out_dir}/artifacts/"
        os.makedirs(self.artifact_dir, exist_ok=True)
        self.artifact_list = []
        self.artifact_dict = {}

        self.artifact_class = None
        self.operator_class = None

        self.operation_list = []

        self.perf_records = []

        self.current_operation = None

        logger.info(f'Creating new Workflow {self.name}')

    def generate_next_label(self):
        """ Generate a new label for the next artifact in the form of "artifact_N" """
        return f"artifact_{len(self)}"

    @abstractmethod
    def initialize_new_artifact(self, label=None, filename=None, schema_map=None) -> Artifact:
        """
        Prepare a new artifact to be added to the workflow
        :param label: Label for the new artifact.
        :param filename: Filename for the artifact to be added.
        :param schema_map: Column Label: Faker Provider mapping the schema for the new artifact.
        :return: Artifact object after initialization
        """
        pass

    def add_artifact(self, artifact: Artifact,
                     from_artifacts: List[Artifact] = None, operation: Operation = None) -> None:
        """
        Add a new artifact to the workflow with label and dataframe.
        Optionally add source label and edge information
        :param artifact: The artifact to be added
        :param from_artifacts: (optional) Source artifacts from which this new artifact was derived
        :param operation: Operation used to derive this new artifact
        """
        self.graph.add_node(artifact.label,
                            **{
                                'schema_map': artifact.schema_map,
                                'file_format': artifact.file_format,
                                'filename': artifact.filename
                            })
        v = artifact.label

        self.artifact_list.append(artifact.label)
        self.artifact_dict[artifact.label] = artifact

        if from_artifacts:
            for u in from_artifacts:
                self.graph.add_edge(u.label, v, **{
                    'code': operation.code,
                })

    def generate_base_artifact(self, num_rows=100, num_cols=10, column_maps=None, label: str = None) -> Artifact:
        """
        Create a base artifact of with given rows and columns
        :param num_rows: number of rows to be generated
        :param num_cols: number of columns to be generated
        :param column_maps: (optional) schema map for the table to be generated
        :param label: (optional) custom label for the artifact to be generated
        :return: Artifact after generation
        """
        if not column_maps:
            column_maps = generate_schema(num_cols)
        if not label:
            label = self.generate_next_label()
        start_time = time.perf_counter()
        new_artifact = self.initialize_new_artifact(label=label, filename=f"{self.artifact_dir}/{label}.csv",
                                                    schema_map=column_maps)
        new_artifact.generate(num_rows, column_maps)
        end_time = time.perf_counter()
        self.add_artifact(new_artifact)

        self.perf_records.append(pd.Series({
            'src': np.nan,
            'dst': label,
            'op': 'generate',
            'args': np.nan,
            'start_time': start_time,
            'end_time': end_time,
            'elapsed_time': end_time - start_time
        }).to_frame().T)

        return new_artifact

    def validate_current_operation(self):
        """
        Ensure that an operation has been initialized before attempting to chain a new operation
        :return: True if operation exists, RuntimeError if not.
        """
        if not self.current_operation:
            logger.error('No current operation in workflow!')
            raise RuntimeError
        return True

    def initialize_operation(self, artifacts: List[Artifact]) -> Operation:
        """
        Initializes a new operation to be performed on "artifacts"
        :param artifacts: List of artifacts to be operated upon.
        :return: Operation object ready to add tranformations to
        """
        self.current_operation = self.operator_class(sources=artifacts, artifact_class=self.artifact_class)
        return self.current_operation

    def chain_to_current_operation(self, op_list: List[Dict]) -> None:
        """
        Add transformation(s) to the currently stacked transformation list
        :param op_list: List of Dicts with "op" and "args" containing all the transforamtions to be stacked.
        :return:
        """
        self.validate_current_operation()
        for op_dict in op_list:
            self.current_operation.chain_operation(op_dict['op'], op_dict['args'])

    def execute_current_operation(self, new_label) -> Artifact:
        """
        Execute the stacked operation to generate a new artifact with label "new_label"
        :param new_label: The new label to assign to the new artifact generated by the operation
        :return: Artifact object representing the new artifact.
        """
        new_artifact = None
        self.validate_current_operation()
        if not new_label:
            new_label = self.generate_next_label()
        try:
            new_artifact = self.current_operation.execute(new_label)
            self.add_artifact(new_artifact, from_artifacts=self.current_operation.sources, operation=self.current_operation)

            # TODO: Exception Handling and return value on op failure / empty df

            # Add operation to op list
            self.operation_list.append(self.current_operation.to_dict())

            # Add performance information
            self.perf_records.append(pd.Series({
                'src': tuple(x.label for x in self.current_operation.sources),
                'dst': self.current_operation.new_label,
                'op_list': '+'.join([x['op'] for x in self.current_operation.op_list]),
                'code': self.current_operation.code,
                'start_time': self.current_operation.start_time,
                'end_time': self.current_operation.end_time,
                'elapsed_time': self.current_operation.get_execution_time()
            }).to_frame().T)

            self.current_operation = None

            return new_artifact

        except (NameError, ValueError) as e:
            logger.error(f'Could not execute Operation: {self.current_operation.op_list}')
            op_dict = self.current_operation.to_dict()
            op_dict['status'] = 'error'
            self.operation_list.append(op_dict)
            self.serialize_workflow()
            raise e

    def generate_artifact_from_operation_list(self, artifacts: List[Artifact], op_list: List[Dict],
                                              new_label: str = None) -> Artifact:
        """
        Generate a new artifact with source artifacts apply operation op with args parameters
        :param new_label: Optional label annotation for the artifact to be generated. Autogenerated if not specified
        :param artifacts: list of source artifacts for the operation to be applied
        :param op_list: list of {op, args} dicts
        :return: Artifact generated after running all the operations.
        """
        if not self.current_operation:
            logger.info('Initializing new operation...')
            self.current_operation = self.initialize_operation(artifacts)

        self.chain_to_current_operation(op_list)
        new_artifact = self.execute_current_operation(new_label)

        return new_artifact

    def serialize_workflow(self, output_dir: str = None) -> None:
        """
        Write out this workflow to directory "output_dir"
        :param output_dir: Directory to write out the workflow to
        :return:
        """
        if not output_dir:
            output_dir = self.out_dir

        # Create Output Directories
        artifact_dir = f"{output_dir}/artifacts/"
        os.makedirs(artifact_dir, exist_ok=True)

        # Write out all artifacts
        for label, artifact in self.artifact_dict.items():
            logger.debug(f"Serialization {label}, {artifact.label}")
            artifact.serialize(filename=f"{artifact_dir}/{label}.{artifact.file_format}")

        # Write out Operation List JSON
        with open(f"{output_dir}/{self.name}_operations.json", 'w') as outfile:
            outfile.write(json.dumps({'name': self.name,
                                      'operation_list': [op for op in self.operation_list]
                                      }, indent=2))

        # Write out Lineage Graph
        nx.write_edgelist(self.graph, f"{output_dir}/{self.name}_gt_graph.csv")

        # Construct Schema Map dict and write out as json
        schema_map_dict = {label: artifact.schema_map for label, artifact in self.artifact_dict.items()}
        with open(f"{output_dir}/{self.name}_schema_map.json", 'w') as outfile:
            outfile.write(json.dumps(schema_map_dict, indent=2))

        # Write out performance table
        self.write_perf()

    @classmethod
    def load_workflow(cls, input_dir: str, out_directory: str, name=None, replay=False,
                      wf_options={}, scale_artifact={}) -> Workflow:
        """
        Load a workflow from disk, usually for replay
        :param input_dir: Input workflow directory
        :param out_directory: Output workflow directory
        :param name: Name for the workflow
        :param replay: Replay the workflow? Default False.
        :param wf_options: Dict of workflow options
        :param scale_artifact: Dict of artifact labels and scale factor if scaling is required.
        :return:
        """
        try:
            artifact_dir = f"{input_dir}/artifacts/"
            operations_file = glob.glob(f"{input_dir}/*_operations.json")[0]

            with open(operations_file, 'r') as infile:
                ops = json.load(infile)

            if name is None:
                name = ops['name']

            workflow = cls(name=name, out_directory=out_directory, **wf_options)

            if replay:
                schema_map_file = glob.glob(f"{input_dir}/*_schema_map.json")[0]
                with open(schema_map_file, 'r') as infile:
                    all_schema_maps = json.load(infile)
                workflow.replay_op_list(artifact_dir, op_list=ops['operation_list'], all_schema_maps=all_schema_maps,
                                        scale_artifact=scale_artifact)
                workflow.write_perf()

            # Revisit copying the workflow graph over, currently replay does this for us.
            # workflow.graph = nx.read_edgelist(f"{input_dir}/{workflow.name}_gt_graph.csv")

            return workflow

        except FileNotFoundError as e:
            logger.error(f"Error Loading Workflow from {input_dir}: {e}")

    def replay_op_list(self, artifact_dir: str, op_list=None, all_schema_maps=None, scale_artifact={}) -> None:
        """
        Replay the operation list given by "op_list" using artifacts in "artifact_dir"
        :param artifact_dir: Directory containing all the artifact
        :param op_list: List of operations to be performed (List of Dicts)
        :param all_schema_maps: Dict containing the schema map for all source artifacts in the workflow
        :param scale_artifact: Scaling factor for each artifact, if needed.
        :return: None
        """
        for opl in op_list:
            for source in opl['sources']:
                if source not in self.artifact_dict.keys():
                    # TODO: Handle PK-FK merges properly - if DF is merge input, we need to maintain the keyspace and
                    # column schema maybe?
                    if source in scale_artifact.keys():
                        logger.info(f"Scaling up Artifact {source} to size {scale_artifact[source]}")
                        source_artifact = self.generate_base_artifact(num_rows=scale_artifact[source],
                                                                      label=source,
                                                                      column_maps=all_schema_maps[source])
                    else:
                        logger.info(f"Loading Pre-Generated Artifact: {source} ")
                        source_artifact = self.initialize_new_artifact(label=source, schema_map=all_schema_maps[source])
                        start_time = time.perf_counter()
                        source_artifact.deserialize(filename=f"{artifact_dir}/{source}.{source_artifact.file_format}")
                        end_time = time.perf_counter()
                        self.perf_records.append(pd.Series({
                            'src': source,
                            'dst': np.nan,
                            'op': 'load',
                            'args': np.nan,
                            'start_time': start_time,
                            'end_time': end_time,
                            'elapsed_time': end_time - start_time
                        }).to_frame().T)
                        self.add_artifact(source_artifact)

            logger.info(f"Replaying Operation List: {tuple(a for a in opl['sources'])} "
                        f"=====> {opl['new_label']}")
            self.generate_artifact_from_operation_list([self.artifact_dict[x] for x in opl['sources']],
                                                       opl['op_list'], new_label=opl['new_label'])

    def write_perf(self, filename=None):
        """
        Write all performance information to filenme
        :param filename: Filename to write performance CSV file to (default is {name}_perf.csv in wf directory
        :return: None
        """
        if not filename:
            filename = f"{self.out_dir}/{self.name}_perf.csv"

        if self.perf_records:
            pd.concat(self.perf_records, ignore_index=True).to_csv(filename)
        else:
            logger.warning('No Performance Data to be Written')

    def select_random_artifact(self, bfactor=0.5, exclude: List[str] = None) -> Artifact:
        """
        Select a random artifact from the current list of artifacts in this workflow
        :param bfactor: Branching factor for exponential probablity in artifact selection (default 0.5)
        :param exclude: List of artifacts to exclude from selection.
        :return: Chosen artifact
        """
        viable_artifacts = dict(filter(lambda x: x[0] not in exclude, self.artifact_dict.items()))
        size = len(viable_artifacts)
        a = np.arange(size)
        prob = (bfactor / (np.exp(bfactor * size) - 1)) * np.exp(bfactor * a)
        prob = prob / prob.sum()

        return self.artifact_dict[np.random.choice(list(viable_artifacts.keys()), 1, p=prob)[0]]

    def __len__(self):
        return len(self.artifact_list)

    def __getitem__(self, item):
        return self.artifact_dict[item]
