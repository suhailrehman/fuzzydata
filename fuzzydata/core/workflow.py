import os

import networkx as nx
from loguru import logger
from glob import glob


class Workflow(nx.DiGraph):
    """
    Class to represent a workflow in fuzzydata, Extends DiGraph from networkx with additional metadata about
    the workflow as required.
    """
    def __init__(self, name='wf', from_dir=None):
        """
        Create a new workflow with a specified name
        :param name: Name of the workflow
        :param from_dir: (optional) Load a workflow previously generated and written to disk from directory
        """
        super(Workflow, self).__init__()

        if from_dir:
            logger.info('Loading Worfklow from {from_dir}', from_dir=from_dir)
            graph_file = next(glob(from_dir+"*.gpkl"))
            reference_graph = nx.read_gpickle(graph_file)
            for node, data in reference_graph.nodes(data=True):
                self.add_node(node, **data)
            for u,v, data in reference_graph.edges(data=True):
                self.add_edge(u, v, **data)

        self.name = name
        logger.info('Creating new Workflow {name}', name=self.name)

    def add_artifact(self, label=None, df=None, from_artifact=None, edge_dict=None):
        """
        Add a new artifact to the workflow with label and dataframe.
        Optionally add source label and edge information
        :param label: artifact label
        :param df: dataframe associated with the artifact
        :param from_artifact: (optional - source artifact this was derived from)
        :param edge_dict: (optional - edge information of artifact)
        """
        self.add_node(label, df=df)
        if from_artifact:
            self.add_edge(from_artifact, label, **edge_dict)

    def to_disk(self, directory='.'):
        """
        Writes the workflow to disk
        :param directory:
        :return:
        """
        os.makedirs(directory, exist_ok=True)
        nx.write_gpickle(super, f"{directory}/{self.name}.gpkl")