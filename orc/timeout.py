import time
from contextlib import contextmanager


@contextmanager
def timeout(timeout_seconds: float):
    """Utility to add timeout check to operations within a loop."""
    start_time = time.time()  # Capture start time

    def check_timeout():
        """Checks if the current time exceeds the specified maximum timeout."""
        elapsed_time = time.time() - start_time
        if elapsed_time > timeout_seconds:
            raise TimeoutError(f"Operation exceeded {timeout_seconds} seconds timeout.")

    try:
        yield check_timeout
    finally:
        pass  # Any necessary cleanup can be handled here if needed
