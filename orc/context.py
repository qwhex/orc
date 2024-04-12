from contextlib import contextmanager
from multiprocessing import Manager

from typing import Any, Generator, Tuple, Callable


class Context:
    def __init__(self):
        self.manager = Manager()
        self.locks = {}
        self.data = self.manager.dict({})
        self.output_queue = self.manager.Queue()

    def add_context_key(self, context_key: str, initializer: Any) -> None:
        if context_key in self.data:
            raise KeyError(f"Context key '{context_key}' already exists")

        self.data[context_key] = initializer
        self.locks[context_key] = self.manager.Lock()

    @contextmanager
    def get_state(self, context_key: str) -> Generator[Tuple[Any, Callable[[Callable[[Any], Any]], Any]], None, None]:
        with self.locks[context_key]:
            aggregate = self.data[context_key]

            def set_val(set_fn):
                updated_val = set_fn(aggregate)
                self.data[context_key] = updated_val
                return updated_val

            yield aggregate, set_val

    def yield_result(self, value: Any) -> None:
        self.output_queue.put(value)
