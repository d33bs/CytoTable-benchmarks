import sys

import pandas as pd

# take an input from sys argsv
input_file = sys.argv[1]

# extract data for joins in pandas
df_image = pd.read_sql(sql="SELECT * FROM Per_Image", con=f"sqlite:///{input_file}")
df_cytoplasm = pd.read_sql(
    sql="SELECT * FROM Per_Cytoplasm", con=f"sqlite:///{input_file}"
)
df_cells = pd.read_sql(sql="SELECT * FROM Per_Cells", con=f"sqlite:///{input_file}")
df_nuclei = pd.read_sql(sql="SELECT * FROM Per_Nuclei", con=f"sqlite:///{input_file}")

# form a merged pandas dataframe
df_joined = (
    # merge image into cytoplasm data
    df_image.merge(right=df_cytoplasm, how="left", on="ImageNumber")
    # merge image + cytoplasm into cells data
    .merge(
        right=df_cells,
        how="left",
        left_on=["ImageNumber", "Cytoplasm_Parent_Cells"],
        right_on=["ImageNumber", "Cells_Number_Object_Number"],
    )
    # merge image + cytoplasm + cells into nuclei data
    .merge(
        right=df_nuclei,
        how="left",
        left_on=["ImageNumber", "Cytoplasm_Parent_Nuclei"],
        right_on=["ImageNumber", "Nuclei_Number_Object_Number"],
    )
)
