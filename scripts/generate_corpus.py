#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Thin wrapper so `python scripts/generate_corpus.py ...` keeps working in a checkout.

The implementation lives in fuzzydata.lineage.corpus_cli, which is installed as the
`fuzzydata-corpus` console script.
"""

import sys

from fuzzydata.lineage.corpus_cli import main

if __name__ == '__main__':
    sys.exit(main())
