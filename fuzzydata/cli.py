# -*- coding: utf-8 -*-

"""
fuzzydata.cli
~~~~~~~~~~~~
This module contains the command-line program to run fuzzydata
:copyright: (c) Suhail Rehman 2022
:license: MIT, see LICENSE for more details.
"""

import shutil
import time
import sys
import argparse
import logging
import os

import json

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fuzzydata.clients import supported_workflows
from fuzzydata.core.generator import generate_workflow

_LOG_LEVELS = {
    'critical': logging.CRITICAL,
    'error': logging.ERROR,
    'warn': logging.WARNING,
    'warning': logging.WARNING,
    'info': logging.INFO,
    'debug': logging.DEBUG
}

_LOG_FORMAT = "%(asctime)s [%(levelname)8s] %(message)s (%(filename)s:%(lineno)s)"


def setup_arguments(args):
    """Processes the args arguments using argparse library

    :param args: list of command line arguments
    :return: options object containing the options listed in args or their defaults.
    """
    parser = argparse.ArgumentParser(prog='fuzzydata')

    parser.add_argument("--wf_client",
                        help=f"Workflow Client to be used (Default pandas). \n"
                             f"Available Workflows: {'|'.join(supported_workflows.keys())}",
                        type=str, default='pandas')

    parser.add_argument("--output_dir",
                        help="Location of Output datasets to be stored",
                        type=str, default='dataset')

    parser.add_argument("--wf_name",
                        help="prefix for each workflow to be generated\
                        dir to be the path prefix for these files.",
                        type=str, default=time.strftime("%Y%m%d-%H%M%S"))

    parser.add_argument("--columns",
                        help="Number of columns in the base version",
                        type=int, default=20)

    parser.add_argument("--rows",
                        help="Number of rows in the base version",
                        type=int, default=1000)

    parser.add_argument("--versions",
                        help="Number of versions to generate",
                        type=int, default=10)

    parser.add_argument("--bfactor",
                        help="Workflow Branching factor, 0.1 is linear, 100 is star-like",
                        type=float, default=5.0)

    parser.add_argument("--matfreq",
                        help="Materialization frequency, i.e. how many operations before writing out an artifact",
                        type=int, default=1)

    parser.add_argument("--log",
                        help="Set Logging Level",
                        type=str, default='info')

    parser.add_argument("--replay_dir",
                        help="Replay existing workflow in directory",
                        type=str)

    parser.add_argument("--wf_options",
                        help="JSON-encoded workflow engine options like sql_string or modin_engine",
                        type=str)

    parser.add_argument("--exclude_ops",
                        help='JSON-encoded list of ops to exclude e.g. ["pivot"]',
                        type=str)

    parser.add_argument("--scale_artifact",
                        help='JSON-encoded dict of {artifact_label: new_size} to be scaled up '
                             'e.g. {"artifact_0" : 1000000}',
                        type=str)

    options = parser.parse_args(args)

    return options


def main(args):
    """ Main entry point into fuzzydata CLI

    :param args: command line arguments
    :return: None
    """
    options = setup_arguments(args)

    # Set log level first
    logging.basicConfig(level=_LOG_LEVELS[options.log.lower()], format=_LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info(f"FuzzyData Config: {options}")

    if os.path.exists(options.output_dir+'/artifacts/'):  # pragma: no cover
        sys.stderr.write(f'\nAn existing workflow exists in directory: {options.output_dir}, Overwrite (Y/N)?:')
        choice = input().lower()
        if choice in {'yes', 'y', 'ye', ''}:
            shutil.rmtree(options.output_dir, )
        else:
            logger.info('Exiting!')
            sys.exit(0)

    wf_options = {}
    exclude_ops = []

    if options.wf_options:
        wf_options = json.loads(options.wf_options)

    if options.exclude_ops:
        exclude_ops = json.loads(options.exclude_ops)

    if options.wf_client not in supported_workflows:
        logger.error(f'{options.wf_client} is not a supported workflow client. Currently supported workflow clients '
                     f'are {supported_workflows.keys()}')
        sys.exit(1)

    if options.replay_dir:  # pragma: no cover
        scale_artifact = {}
        if options.scale_artifact:
            scale_artifact = json.loads(options.scale_artifact)
        logger.info(f'Replaying Previous Workflow from directory {options.replay_dir}')
        workflow = supported_workflows[options.wf_client].load_workflow(input_dir=options.replay_dir,
                                                                        out_directory=options.output_dir,
                                                                        name=options.wf_name,
                                                                        replay=True,
                                                                        wf_options=wf_options,
                                                                        scale_artifact=scale_artifact)
        workflow.serialize_workflow()

    else:
        workflow = generate_workflow(workflow_class=supported_workflows[options.wf_client],
                                     name=options.wf_name, num_versions=options.versions,
                                     base_shape=(options.columns, options.rows),
                                     out_directory=options.output_dir, bfactor=options.bfactor,
                                     wf_options=wf_options,
                                     exclude_ops=exclude_ops, matfreq=options.matfreq)

        # Generate Workflow calls serialize at the end.

    logger.info(f'Workflow generation completed and written to directory: {workflow.out_dir}')
    logger.info(f'To rerun this workflow in the future, use the following command:\n\nfuzzydata --replay_dir={workflow.out_dir}\n\n')


if __name__ == '__main__':
    main(sys.argv[1:])
