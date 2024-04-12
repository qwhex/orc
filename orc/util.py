from typing import Generator, List, TypeVar, Iterable

Item = TypeVar('Item')


def batch_generator(input_stream: Iterable[Item], batch_count: int) -> Generator[List[Item], None, None]:
    """Yields batches of the specified size from the input stream."""
    batch = []
    for item in input_stream:
        batch.append(item)
        if len(batch) == batch_count:
            yield batch
            batch = []
    if batch:  # Yield any remaining items as the last batch
        yield batch
