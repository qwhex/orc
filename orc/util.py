from typing import Generator, List, Any


def batch_generator(input_stream: Generator[Any, None, None], batch_count: int) -> Generator[List[Any], None, None]:
    """Yields batches of the specified size from the input stream."""
    batch = []
    for item in input_stream:
        batch.append(item)
        if len(batch) == batch_count:
            yield batch
            batch = []
    if batch:  # Yield any remaining items as the last batch
        yield batch
