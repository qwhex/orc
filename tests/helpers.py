import random
import time
from typing import Any, Optional

from orc.pipeline import ReducerReturn


def sleep_x(x: int) -> None:
    sleep_time = min(0.1, x / 1000.0)
    time.sleep(sleep_time)


def init_zero() -> int:
    return 0


def init_set() -> set:
    return set()


def slow_double(x: int) -> int:
    sleep_x(x)
    return x * 2


def slow_tenth(x: int) -> int:
    sleep_x(x)
    return x // 10


def only_one_for_tenths(x: int, agg: set) -> ReducerReturn[set]:
    sleep_x(x)

    tenth = x // 10
    if tenth in agg:
        return None, None

    def acc(prev: set) -> set:
        return prev.union({tenth})

    return x, acc


#

def double(x: int) -> int:
    return x * 2


def to_str(item: int) -> str:
    return f'item_{item}'


def half_int(item: int) -> int:
    return item // 2


def tenth(x: int) -> int:
    return x // 10


def identity(x: Any) -> Any:
    return x


def accumulate(x: int, agg: int) -> ReducerReturn[int]:
    return x + agg, lambda prev: prev + x


# filter

def unique_filter(x: Any, agg: set) -> ReducerReturn[set]:
    if x in agg:
        return None
    return x, lambda prev: prev.union({x})


unique_filter.init = set


def even_filter(x: int) -> Optional[int]:
    if x % 2 == 0:
        return x
