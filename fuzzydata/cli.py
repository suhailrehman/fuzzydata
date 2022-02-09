import time
import sys
import argparse
import logging
import os

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
    parser = argparse.ArgumentParser()

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
                        type=int, default=100)

    parser.add_argument("--bfactor",
                        help="Workflow Branching factor, 0.1 is linear, 100 is star-like",
                        type=float, default=5.0)

    parser.add_argument("--matfreq",
                        help="Materialization frequency, i.e. how many operations before writing out an artifact",
                        type=int, default=1)

    parser.add_argument("--npp",
                        help="Generate merges, group-bys and pivots",
                        type=bool, default=True)

    parser.add_argument("--log",
                        help="Set Logging Level",
                        type=str, default='info')

    options = parser.parse_args(args)

    return options


def main(args):
    options = setup_arguments(args)

    # Set log level first
    logging.basicConfig(level=_LOG_LEVELS[options.log.lower()], format=_LOG_FORMAT)
    logger = logging.getLogger(__name__)

    logger.info(f"FuzzyData Config: {options}")

    workflow = generate_workflow(workflow_class=supported_workflows[options.wf_client],
                                 name=options.wf_name, num_versions=options.versions,
                                 base_shape=(options.columns, options.rows),
                                 out_directory=options.output_dir, bfactor=options.bfactor)
    # matfreq=options.matfreq)

    logger.info(f'Workflow generation completed and written to directory: {workflow.out_dir}')


if __name__ == '__main__':
    main(sys.argv[1:])
