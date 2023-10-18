"""
Demonstrating CytoTable capabilities with input datasets
"""
import cytotable

import shutil
from pathlib import Path
import sys

# take an input from sys argsv
input_file = sys.argv[1]

# Get the path of the current module an use it as a subdir
dest_path = f"{Path(__file__).parent.resolve()}/temp_example"

result = cytotable.convert(
    source_path=input_file,
    dest_path=dest_path,
    dest_datatype="parquet",
    source_datatype="sqlite",
    preset="cellprofiler_sqlite_pycytominer",
)

shutil.rmtree(path=dest_path, ignore_errors=True)
