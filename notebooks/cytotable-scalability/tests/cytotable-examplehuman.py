"""
Demonstrating CytoTable capabilities with ExampleHuman data from:
https://cellprofiler.org/examples
"""
import cytotable

import shutil
from pathlib import Path

# Get the path of the current module
module_path = Path(__file__).parent.resolve()


dest_path = f"{module_path}/temp_example"

cellprofiler_ExampleHuman_data = f"{module_path}/CytoTable-clone/tests/data/cellprofiler/ExampleHuman"

result = cytotable.convert(
    source_path=cellprofiler_ExampleHuman_data,
    dest_path=dest_path,
    dest_datatype="parquet",
    source_datatype="csv",
    preset="cellprofiler_csv",
)

shutil.rmtree(path=dest_path, ignore_errors=True)
