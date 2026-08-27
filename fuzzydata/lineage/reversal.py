# -*- coding: utf-8 -*-

"""
fuzzydata.lineage.reversal
~~~~~~~~~~~~~~~~~~~~~~~~~~

Reversal-closed instance pairs for orientation-sensitivity benchmarks.

A *reversal-closed* pair consists of two workflows over the same set of
artifacts, differing only in the direction of every provenance edge.
Because every operator in REVERSAL_CLOSED_OPS is invertible, both
directions represent valid derivations.  A lineage method that is
insensitive to direction will place the two workflows in the same
equivalence class — which is wrong by construction.

Usage::

    original_wf, reversed_wf = generate_reversal_pair(
        DataFrameWorkflow, base_seed=42, tmp_dir='/tmp/rc', n_artifacts=5)
"""

import os
from typing import Tuple

#: Operators whose inverse also produces a valid derivation of the source.
#: Only these may appear in a reversal-closed workflow so the reversed
#: direction is physically realizable.
REVERSAL_CLOSED_OPS = frozenset({'sort', 'shuffle', 'rename', 'apply'})

#: Ops to exclude when calling generate_workflow so only REVERSAL_CLOSED_OPS
#: appear in the candidate set.
_EXCLUDE_FOR_REVERSAL = [
    'project', 'select', 'dropna', 'dedupe', 'sample',
    'train_test_split', 'groupby', 'pivot', 'normalize',
    'standardize', 'label_encode', 'merge', 'one_hot_encode',
    'fill', 'astype',
]


def generate_reversal_pair(workflow_class, base_seed: int, tmp_dir: str,
                           n_artifacts: int = 4) -> Tuple:
    """Generate a reversal-closed pair of workflows.

    Both workflows contain the same set of artifact DataFrames; they differ
    only in which endpoint is considered the source and which the destination
    on every provenance edge.

    :param workflow_class: e.g. DataFrameWorkflow.
    :param base_seed: seed for the forward workflow.
    :param tmp_dir: parent directory; two sub-directories are created inside.
    :param n_artifacts: number of artifacts (including the base); kept small
        because a chain of all-invertible ops on tiny frames is enough.
    :return: (original_wf, reversed_wf)
    """
    from fuzzydata.core.generator import generate_workflow

    fwd_dir = os.path.join(tmp_dir, 'original')
    rev_dir = os.path.join(tmp_dir, 'reversed')
    os.makedirs(fwd_dir, exist_ok=True)
    os.makedirs(rev_dir, exist_ok=True)

    # Generate the forward workflow restricted to invertible ops only.
    original_wf = generate_workflow(
        workflow_class,
        name='reversal_original',
        num_versions=n_artifacts,
        base_shape=(5, 30),
        out_directory=fwd_dir,
        matfreq=1,
        seed=base_seed,
        topology='chain',
        exclude_ops=_EXCLUDE_FOR_REVERSAL,
    )

    # Build the reversed workflow: same artifacts, edges flipped.
    reversed_wf = workflow_class(
        name='reversal_reversed',
        out_directory=rev_dir,
        file_format=original_wf.file_format,
    )

    # Copy every artifact into the reversed workflow (same DataFrames).
    for label, artifact in original_wf.artifact_dict.items():
        new_artifact = reversed_wf.initialize_new_artifact(
            label=label,
            filename=reversed_wf.artifact_path(label),
            schema_map=artifact.schema_map,
        )
        new_artifact.from_df(artifact.to_df())
        new_artifact.schema_map = artifact.schema_map
        reversed_wf.artifact_list.append(label)
        reversed_wf.artifact_dict[label] = new_artifact
        reversed_wf.graph.add_node(label, schema_map=artifact.schema_map,
                                   file_format=reversed_wf.file_format,
                                   filename=reversed_wf.artifact_path(label))

    # Replay the operation list with source/destination swapped.
    for op_entry in reversed(original_wf.operation_list):
        orig_sources = op_entry['sources']   # list of label strings
        orig_dest = op_entry['new_label']    # label string
        op_list = op_entry['op_list']

        # In the reversed workflow: orig_dest is the source, orig_sources[0] is the new artifact.
        # Only chain edges (single source) are in a reversal-closed workflow by construction.
        rev_source_label = orig_dest
        rev_dest_label = orig_sources[0]

        annotation = dict(op_entry.get('annotation', {}))
        annotation['reversed'] = True

        reversed_wf.operation_list.append({
            'sources': [rev_source_label],
            'new_label': rev_dest_label,
            'op_list': op_list,
            'annotation': annotation,
        })
        reversed_wf.graph.add_edge(rev_source_label, rev_dest_label)

    reversed_wf.metadata = dict(original_wf.metadata or {})
    reversed_wf.metadata['reversal_of'] = original_wf.name

    reversed_wf.serialize_workflow()

    return original_wf, reversed_wf
