from typing import Generator, List, TypeVar, Iterable, Callable

Item = TypeVar('Item')
T = TypeVar('T')


def batch_generator(input_stream: Iterable[Item], batch_count: int) -> Generator[List[Item], None, None]:
    """
    Yield batches of the specified size from the input stream
    """
    batch = []
    for item in input_stream:
        batch.append(item)
        if len(batch) == batch_count:
            yield batch
            batch = []
    if batch:
        # yield the last partial batch
        yield batch
