import logging
from typing import Dict, List

import pandas

from fuzzydata.core.artifact import Artifact
from fuzzydata.core.generator import generate_table
from fuzzydata.core.operation import Operation, T
from fuzzydata.core.workflow import Workflow

logger = logging.getLogger(__name__)


#: Emitted into the exported script as well, so generated code stands alone.
FLATTEN_PIVOT_SRC = '''
def _flatten_pivot(df):
    """Flatten a pivot_table result to single-level, string-named columns.

    pivot_table with index/columns/values returns a MultiIndex on the columns. Such a frame
    cannot survive a CSV round-trip and no downstream operator can address its columns, so
    every pivot artifact was a dead end. Joining the levels keeps the value column and the
    pivoted key visible in the name.
    """
    df = df.reset_index()
    if hasattr(df.columns, "nlevels") and df.columns.nlevels > 1:
        df.columns = ["__".join(str(p) for p in tup if str(p) != "") for tup in df.columns]
    else:
        df.columns = [str(c) for c in df.columns]
    return df
'''

exec(FLATTEN_PIVOT_SRC)


class DataFrameArtifact(Artifact):

    def __init__(self, *args, **kwargs):
        self.pd = kwargs.pop("pd", pandas)
        from_df = kwargs.pop("from_df", None)
        super(DataFrameArtifact, self).__init__(*args, **kwargs)
        self._deserialization_function = {
            'csv': self.pd.read_csv,
            'parquet': self.pd.read_parquet,
        }
        self._serialization_function = {
            'csv': 'to_csv',
            'parquet': 'to_parquet',
        }

        self.operation_class = DataFrameOperation
        self.table = None
        self.in_memory = False

        if from_df is not None:
            self.from_df(from_df)

    def generate(self, num_rows, schema, rng=None):
        self.table = generate_table(num_rows, column_dict=schema, pd=self.pd, rng=rng)
        self.schema_map = dict(schema)
        self.in_memory = True

    def from_df(self, df):
        self.table = self.pd.DataFrame(df)
        self.in_memory = True

    def deserialize(self, filename=None):
        if not filename:
            filename = self.filename

        self.table = self._deserialization_function[self.file_format](filename)
        self.in_memory = True

    def serialize(self, filename=None):
        if not filename:
            filename = self.filename

        if self.in_memory:
            serialization_method = getattr(self.table, self._serialization_function[self.file_format])
            # index=False: to_csv writes the index by default, so every serialize/deserialize
            # round-trip injected an "Unnamed: 0" column. That broke replay fidelity and put
            # a phantom, perfectly-correlated column into every artifact written to disk.
            serialization_method(filename, index=False)

    def destroy(self):
        del self.table

    def to_df(self) -> pandas.DataFrame:
        return self.table

    def __len__(self):
        if self.in_memory:
            return len(self.table.index)


class DataFrameOperation(Operation['DataFrameArtifact']):
    def __init__(self, *args, **kwargs):
        self.artifact_class = kwargs.pop('artifact_class', DataFrameArtifact)
        super(DataFrameOperation, self).__init__(*args, **kwargs)
        self.code = 'self.sources[0].table' # Starting point for chained code generation.

    def apply(self, numeric_col: str, a: float, b: float) -> DataFrameArtifact:
        super(DataFrameOperation, self).apply(numeric_col, a, b)
        new_col_name = self.apply_column_name(numeric_col, a, b)
        # .assign(**{...}) with string keys and x["col"] indexing, rather than
        # .assign(name = lambda x: x.col ...). Column prefixes are drawn from
        # ascii_letters+digits, so a label can start with a digit (e.g. 62CZ7__pyfloat),
        # which is not a valid Python identifier -- and this string is eval()'d.
        return f'.assign(**{{"{new_col_name}": lambda x: x["{numeric_col}"]*{a}+{b}}})'

    def sample(self, frac: float, random_state: int = None) -> DataFrameArtifact:
        super(DataFrameOperation, self).sample(frac, random_state)
        if random_state is None:
            return f'.sample(frac={frac})'
        return f'.sample(frac={frac}, random_state={random_state})'

    def groupby(self, group_columns: List[str], agg_columns: List[str], agg_function: str) -> T:
        super(DataFrameOperation, self).groupby(group_columns, agg_columns, agg_function)
        logger.debug(f"Groupby on {self.sources[0].label} : {group_columns}/{agg_columns}")
        return f'[{group_columns+agg_columns}].groupby({group_columns}).{agg_function}().reset_index()'

    def project(self, output_cols: List[str]) -> T:
        super(DataFrameOperation, self).project(output_cols)
        return f'[{output_cols}]'

    def select(self, condition: str) -> T:
        super(DataFrameOperation, self).select(condition)
        return f'.query("{condition}")'

    def merge(self, key_col: List[str]) -> T:
        super(DataFrameOperation, self).merge(key_col)
        return f'.merge(self.sources[1].table, on="{key_col}")'

    def pivot(self, index_cols: List[str], columns: List[str], value_col: List[str], agg_func: str) -> T:
        super(DataFrameOperation, self).pivot(index_cols, columns, value_col, agg_func)
        return (f'.pivot_table(index={index_cols}, columns={columns},values={value_col},'
                f'aggfunc="{agg_func}").pipe(_flatten_pivot)')

    def fill(self, col_name: str, old_value, new_value):
        super(DataFrameOperation, self).fill(col_name, old_value, new_value)
        # repr() here, not at the call site: this string is eval()'d, so the values need to
        # be Python literals. The SQL client quotes the same raw values its own way.
        return f'.replace({{ "{col_name}": {old_value!r} }}, {new_value!r})'

    # ------------------------------------------------------------------ 0.1.0 operators
    # Column labels are addressed with x["col"] / dict literals throughout, never as
    # attributes or identifiers: prefixes are drawn from ascii_letters+digits so a label can
    # begin with a digit, and every string here is eval()'d.

    def dropna(self, subset: List[str] = None) -> T:
        super(DataFrameOperation, self).dropna(subset)
        if subset:
            return f'.dropna(subset={list(subset)})'
        return '.dropna()'

    def dedupe(self, subset: List[str] = None) -> T:
        super(DataFrameOperation, self).dedupe(subset)
        if subset:
            return f'.drop_duplicates(subset={list(subset)})'
        return '.drop_duplicates()'

    def rename(self, column_map: Dict[str, str]) -> T:
        super(DataFrameOperation, self).rename(column_map)
        return f'.rename(columns={dict(column_map)})'

    def astype(self, column: str, dtype: str) -> T:
        super(DataFrameOperation, self).astype(column, dtype)
        return f'.astype({{"{column}": "{dtype}"}})'

    def normalize(self, column: str) -> T:
        super(DataFrameOperation, self).normalize(column)
        # Guard the degenerate constant-column case: max == min would divide by zero and
        # silently fill the column with NaN.
        return (f'.assign(**{{"{column}": lambda x: '
                f'(x["{column}"] - x["{column}"].min()) / '
                f'((x["{column}"].max() - x["{column}"].min()) or 1)}})')

    def standardize(self, column: str) -> T:
        super(DataFrameOperation, self).standardize(column)
        return (f'.assign(**{{"{column}": lambda x: '
                f'(x["{column}"] - x["{column}"].mean()) / '
                f'(x["{column}"].std() or 1)}})')

    def label_encode(self, column: str) -> T:
        super(DataFrameOperation, self).label_encode(column)
        # factorize() is deterministic in order of first appearance, so this replays.
        return f'.assign(**{{"{column}": lambda x: x["{column}"].factorize()[0]}})'

    def one_hot_encode(self, column: str, categories: List[str]) -> T:
        super(DataFrameOperation, self).one_hot_encode(column, categories)
        # Build the indicator columns from the recorded category list rather than from
        # pd.get_dummies, so the destination schema is fixed by the spec and not by the data.
        assigns = ', '.join(
            f'"{self.one_hot_column_name(column, c)}": '
            f'lambda x, _c={c!r}: (x["{column}"] == _c).astype(int)'
            for c in categories)
        return f'.assign(**{{{assigns}}}).drop(columns=["{column}"])'

    def train_test_split(self, frac: float, random_state: int, side: str) -> T:
        super(DataFrameOperation, self).train_test_split(frac, random_state, side)
        train = f'.sample(frac={frac}, random_state={random_state})'
        if side == 'train':
            return train
        # The complement, not a second draw: drop exactly the rows the train side took.
        return (f'.pipe(lambda d: d.drop(index=d{train}.index))')

    def chain_operation(self, op, args):
        self.code += getattr(self, op)(**args)
        super(DataFrameOperation, self).chain_operation(op, args)

    def materialize(self, new_label):
        new_df = eval(self.code)
        super(DataFrameOperation, self).materialize(new_label)
        return self.artifact_class(label=self.new_label,
                                   from_df=new_df,
                                   schema_map=self.resolve_schema_map(new_df))
    
    @property
    def export_code(self):
        ''' Returns a string representation of code to run outside fuzzydata'''
        code = self.code
        for ix in range(len(self.sources)):
            code = code.replace(f'self.sources[{ix}].table', self.sources[ix].label)
        return code

class DataFrameWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super(DataFrameWorkflow, self).__init__(*args, **kwargs)
        self.artifact_class = DataFrameArtifact
        self.operator_class = DataFrameOperation
        self.wf_code_export = "import pandas as pd\n" + FLATTEN_PIVOT_SRC + "\n"

    def initialize_new_artifact(self, label=None, filename=None, schema_map=None):
        return DataFrameArtifact(label, filename=filename, schema_map=schema_map,
                                 file_format=self.file_format)
    

    def add_artifact(self, artifact: Artifact,
                    from_artifacts: List[Artifact] = None, operation: Operation = None) -> None:
        """ Override to add code export to workflow."""
        super(DataFrameWorkflow, self).add_artifact(artifact, from_artifacts, operation)
        if from_artifacts:
            self.wf_code_export += f"{self.artifact_list[-1]} = {operation.export_code}\n"
        else:
            reader = 'read_parquet' if artifact.file_format == 'parquet' else 'read_csv'
            self.wf_code_export += (f"{artifact.label} = pd.{reader}"
                                    f"('artifacts/{artifact.label}.{artifact.file_format}')\n")

    def serialize_workflow(self, output_dir: str = None) -> None:
        """ Override to add code export to workflow."""
        super(DataFrameWorkflow, self).serialize_workflow(output_dir)
        if not output_dir:
            output_dir = self.out_dir
        # Write out Generated code
        with open(f"{output_dir}/{self.name}_code.py", 'w') as outfile:
            outfile.write(self.wf_code_export)
    