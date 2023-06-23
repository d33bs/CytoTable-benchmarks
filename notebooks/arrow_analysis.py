# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.6
#   kernelspec:
#     display_name: Python 3 (ipyflow)
#     language: python
#     name: ipyflow
# ---

# # Why arrow?
#
# This notebook explores the benefits or drawbacks of using the [arrow](https://arrow.apache.org) in-memory data format relative to other formats such as Pandas DataFrames.

# set ipyflow reactive mode
# %flow mode reactive

# +
import pathlib

import duckdb
import numpy as np
import pandas as pd
import plotly.express as px
import polars as pl
import pyarrow as pa
from pyarrow import parquet
from pympler.asizeof import asizeof
from utilities import timer

# -

# target file or table names
parquet_name = "example.parquet"

# remove any existing prior work
pathlib.Path(parquet_name).unlink(missing_ok=True)

# +
# starting rowcount and col count
nrows = 10
ncols = 5

# result list for storing data
results = []

# loop for iterating over increasingly large dataframes
# and gathering data about operations on them
for _ in range(1, 9):
    # increase the size of the dataframe
    nrows *= 2
    ncols *= 2

    # form a dataframe using randomized data
    df = pd.DataFrame(
        np.random.rand(nrows, ncols), columns=[f"col_{num}" for num in range(0, ncols)]
    )
    # write to parquet for tests below
    df.to_parquet(path=parquet_name, compression="snappy")

    # append data to the result list
    results.append(
        {
            # general information about the dataframe
            "dataframe_shape (rows, cols)": str(df.shape),
            # information about pandas
            "pandas_read_time (secs)": timer(pd.read_parquet, path=parquet_name),
            "pandas_size (bytes)": asizeof(pd.read_parquet(path=parquet_name)),
            # information about pyarrow
            "pyarrow_read_time (secs)": timer(parquet.read_table, source=parquet_name),
            "pyarrow_size (bytes)": asizeof(parquet.read_table(source=parquet_name)),
            # information about polars
            "polars_read_time (secs)": timer(
                pl.scan_parquet, source=parquet_name, method_chain="collect"
            ),
            "polars_size (bytes)": pl.scan_parquet(source=parquet_name)
            .collect()
            .estimated_size(),
            # information about duckdb numpy
            "duckdb_arrow_read_time (secs)": timer(
                duckdb.connect().execute,
                query=f"SELECT * FROM read_parquet('{parquet_name}')",
                method_chain="arrow",
            ),
            "duckdb_arrow_size (bytes)": asizeof(
                duckdb.connect()
                .execute(query=f"SELECT * FROM read_parquet('{parquet_name}')")
                .arrow()
            ),
        }
    )

    # remove any existing files in preparation for next steps
    pathlib.Path(parquet_name).unlink(missing_ok=True)


df_results = pd.DataFrame(results)
df_results
# -
# write times barchart
fig = px.bar(
    df_results,
    x=[
        "pandas_read_time (secs)",
        "pyarrow_read_time (secs)",
        "polars_read_time (secs)",
        "duckdb_arrow_read_time (secs)",
    ],
    y="dataframe_shape (rows, cols)",
    orientation="h",
    barmode="group",
    labels={"dataframe_shape (rows, cols)": "DataFrame Shape", "value": "Seconds"},
    title="How long are read times for different formats?",
)
fig.show()


# write times barchart
fig = px.bar(
    df_results,
    x=[
        "pandas_size (bytes)",
        "pyarrow_size (bytes)",
        "polars_size (bytes)",
        "duckdb_arrow_size (bytes)",
    ],
    y="dataframe_shape (rows, cols)",
    orientation="h",
    barmode="group",
    labels={"dataframe_shape (rows, cols)": "DataFrame Shape", "value": "Bytes"},
    title="What is the memory size for different formats?",
)
fig.show()
