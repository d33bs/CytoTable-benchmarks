import sys

"""
An example using duckdb to join input data from an argument.
"""

import duckdb

# take an input from sys argsv
input_file = sys.argv[1]

sql_statement = f"""
/* install and load the sqlite extension to access file data */
INSTALL sqlite;
LOAD sqlite;
CALL sqlite_attach('{input_file}');

SELECT
    *
FROM
    Per_Image
/* join image into cytoplasm data */
LEFT JOIN Per_Cytoplasm ON
    Per_Cytoplasm.ImageNumber = Per_Image.ImageNumber
/* join image + cytoplasm data into cells data */
LEFT JOIN Per_Cells ON
    Per_Cells.ImageNumber = Per_Cytoplasm.ImageNumber
    AND Per_Cells.Cells_Number_Object_Number = Per_Cytoplasm.Cytoplasm_Parent_Cells
/* join image + cytoplasm + cells data into nuclei data */
LEFT JOIN Per_Nuclei ON
    Per_Nuclei.ImageNumber = Per_Cytoplasm.ImageNumber
    AND Per_Nuclei.Nuclei_Number_Object_Number = Per_Cytoplasm.Cytoplasm_Parent_Nuclei
"""

# form a joined table
df_joined = duckdb.connect().execute(sql_statement).df()
