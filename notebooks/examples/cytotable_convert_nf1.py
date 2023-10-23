"""
Demonstrating CytoTable capabilities with input datasets
"""
import cytotable

import pathlib
import sys

# take an input from sys argsv
input_file = sys.argv[1]

# Get the path of the current module an use it as a subdir
dest_path = f"{pathlib.Path(__file__).parent.resolve()}/{pathlib.Path(input_file).name}"

result = cytotable.convert(
    source_path=input_file,
    dest_path=dest_path,
    dest_datatype="parquet",
    source_datatype="sqlite",
    preset="cellprofiler_sqlite_pycytominer",
    chunk_size=200000,
)

pathlib.Path(dest_path).unlink()
