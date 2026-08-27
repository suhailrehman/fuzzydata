# -*- coding: utf-8 -*-

"""
fuzzydata.lineage
~~~~~~~~~~~~

Lineage-corpus features: real-table profiling, provenance annotations, equivalence
classes and the corpus driver.

Kept as a separate namespace from fuzzydata.core on purpose. core/ is the workflow
fuzzer; this package is the corpus generator built on top of it. The seam keeps the
"fuzz tester or corpus generator?" question answerable without a refactor.
"""
