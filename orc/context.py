from contextlib import contextmanager
from multiprocessing import Manager, Lock
from multiprocessing.managers import SyncManager, DictProxy
from queue import Queue
from typing import Any, Generator, Tuple, Callable, TypeVar, Generic, Dict

Item = TypeVar('Item')
State = Tuple[Item, Callable[[Item], Item]]


class Context(Generic[Item]):
    def __init__(self):
        self.manager: SyncManager = Manager()
        self.locks: Dict[str, Lock] = {}
        self.data: DictProxy[str, Any] = self.manager.dict({})
        self.output_queue: Queue[Item] = self.manager.Queue()

    def add_context_key(self, context_key: str, initializer: Any) -> None:
        if context_key in self.data:
            raise KeyError(f"Context key '{context_key}' already exists")

        self.data[context_key] = initializer
        self.locks[context_key] = self.manager.Lock()

    @contextmanager
    def get_state(self, context_key: str) -> Generator[State, None, None]:
        with self.locks[context_key]:
            aggregate = self.data[context_key]

            def set_val(set_fn):
                updated_val = set_fn(aggregate)
                self.data[context_key] = updated_val
                return updated_val

            yield aggregate, set_val

    def yield_result(self, value: Item) -> None:
        self.output_queue.put(value)
