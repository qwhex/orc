import time
from functools import wraps
from typing import Callable

from multiprocessing import Process
from typing import Any, Tuple, Callable, TypeVar, Optional, Generic, Iterable


def timeout(max_timeout: float):
    """Utility to add timeout to slow operations"""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time
            if elapsed_time > max_timeout:
                raise TimeoutError(f"Function exceeded {max_timeout} seconds timeout.")
            return result

        return wrapper

    return decorator
