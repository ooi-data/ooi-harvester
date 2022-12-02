"""
Core utility functions
"""
from typing import Tuple

def prefect_version() -> Tuple[int, int, int]:
    """Get prefect version"""
    import prefect
    return tuple([int(n) for n in prefect.__version__.split(".")])
