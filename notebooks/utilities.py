"""
Utilities for running various benchmarks.
"""
import pathlib
import time
from typing import Callable, Optional

import requests


def timer(func: Callable, method_chain: Optional[str] = None, *args, **kwargs) -> float:
    """
    A timer function which runs a function and related arguments
    to return the total time in seconds which were taken for completion.
    """

    # find the start time
    start_time = time.time()

    # run the function with given args
    result = func(*args, **kwargs)

    # chain the result to the specified method
    if method_chain is not None:
        result = getattr(result, method_chain)()

    # return the current time minus the start time
    return time.time() - start_time


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
