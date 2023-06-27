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

# # Create Example Data
#
# Create example data for use in other work within this repo.

# set ipyflow reactive mode
# %flow mode reactive

# +
import pathlib
import shutil
import sqlite3

import requests

# -

url = "https://github.com/cytomining/CytoTable/blob/main/tests/data/cellprofiler/NF1_SchwannCell_data/all_cellprofiler.sqlite?raw=true"
orig_filepath = "./examples/data/all_cellprofiler.sqlite"


def download_file(urlstr, filename):
    """
    Download a file given a string url
    """
    if pathlib.Path(filename).exists():
        print("We already have downloaded the file!")
        return

    # Send a HTTP request to the URL of the file you want to access
    response = requests.get(urlstr, timeout=30)

    # Check if the request was successful
    if response.status_code == 200:
        with open(filename, "wb") as file:
            # Write the contents of the response to a file
            file.write(response.content)
    else:
        print(f"Failed to download file, status code: {response.status_code}")


# download the original file
download_file(url, orig_filepath)


def double_database_size(filename: str):
    """
    A function for doubling the size of the database given a filename.
    Note: unique to CellProfiler SQLite output and accounts for
    various unique keys.
    """
    print(filename)

    # Connect to the SQLite database
    with sqlite3.connect(filename) as conn:
        # Use sqlite3.Row to access columns by name
        conn.row_factory = sqlite3.Row

        # Create a cursor
        cur = conn.cursor()

        for tablename in ["Per_Image", "Per_Cytoplasm", "Per_Nuclei", "Per_Cells"]:
            print(
                f"Start count {tablename}: {dict(cur.execute(f'SELECT count(*) FROM {tablename}').fetchall()[0])}"
            )
            # Select all rows from the table
            cur.execute(f"SELECT * FROM {tablename}")
            rows = cur.fetchall()

            # Find the maximum id in the existing data
            max_id = max(row["ImageNumber"] for row in rows)

            # Copy the rows and increment the id values
            new_rows = []
            for row in rows:
                new_row = dict(row)
                new_row["ImageNumber"] += max_id
                new_rows.append(new_row)

            # Insert the new rows into the table
            for row in new_rows:
                placeholders = ", ".join("?" * len(row))
                columns = ", ".join(row.keys())
                cur.execute(
                    f"INSERT INTO {tablename} ({columns}) VALUES ({placeholders})",
                    list(row.values()),
                )

            print(
                f"End count {tablename}: {dict(cur.execute(f'SELECT count(*) FROM {tablename}').fetchall()[0])}"
            )


# # copy the original with new name
doubled_file = shutil.copy(
    orig_filepath, orig_filepath.replace(".sqlite", "-x2.sqlite")
)
doubled_file

double_database_size(filename=doubled_file)

# # copy the original with new name
quadrupled_file = shutil.copy(
    doubled_file, orig_filepath.replace(".sqlite", "-x4.sqlite")
)
quadrupled_file

double_database_size(filename=quadrupled_file)

# # copy the original with new name
octupled_file = shutil.copy(
    quadrupled_file, orig_filepath.replace(".sqlite", "-x8.sqlite")
)
octupled_file

double_database_size(filename=octupled_file)

# # copy the original with new name
hexadecupled_file = shutil.copy(
    octupled_file, orig_filepath.replace(".sqlite", "-x16.sqlite")
)
hexadecupled_file

double_database_size(filename=hexadecupled_file)
