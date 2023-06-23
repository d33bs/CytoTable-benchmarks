"""
Utilities for running various benchmarks.
"""
import time
from typing import Callable, Optional


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
