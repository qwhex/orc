import random
from time import sleep
from typing import Callable

import pytest

from orc.pipeline import Context, Pipeline


def double(x):
    sleep(random.uniform(0, 0.1))
    return x * 2


def tenth(x):
    sleep(random.uniform(0, 0.1))
    return x // 10


def only_one_for_tenths(x, aggregate):
    sleep(random.uniform(0, 0.1))
    tenth = x // 10
    if tenth in aggregate:
        return None, None
    else:
        return x, lambda prev: prev.union({tenth})


# -----
# Tests
# -----

def test_context_initialization():
    ctx = Context()
    assert len(ctx.data) == 0
    assert ctx.output_queue.empty()


def test_add_context_key():
    ctx = Context()
    ctx.add_context_key('test_key', 0)
    assert 'test_key' in ctx.data
    assert ctx.data['test_key'] == 0
    assert 'test_key' in ctx.locks


def test_map_operation():
    pipeline = Pipeline()
    pipeline.map('double', double)
    assert pipeline.operations == [('map', 'double', double)]


def test_reduce_operation():
    pipeline = Pipeline()
    pipeline.reduce('test_reduce', only_one_for_tenths, set())
    assert pipeline.operations == [('reduce', 'test_reduce', only_one_for_tenths)]
    assert 'test_reduce' in pipeline.context.data
    assert pipeline.context.data['test_reduce'] == set()


@pytest.fixture
def basic_pipeline():
    pipeline = Pipeline()
    pipeline.reduce('only_one_for_tenths', only_one_for_tenths, set()).map('tenth', tenth)
    return pipeline


def test_pipeline_execution(basic_pipeline):
    input_stream = iter([0, 1, 10, 11, 20, 21])
    result = list(basic_pipeline.run(input_stream, 1))
    expected_result = [0, 1, 2]  # Example expected result, adjust as needed
    assert result == expected_result


def test_concurrency_management(basic_pipeline):
    with pytest.raises(ValueError):
        list(basic_pipeline.run(iter([1, 2, 3]), 2))
        list(basic_pipeline.run(iter([4, 5, 6]), 2))  # This should raise an error as the pipeline has already run


#
# Invalid calls
#

# List of assertions:
"""
Test invalid number of processes: Checks that the pipeline raises a ValueError when attempting to run with zero processes.
Test empty input stream: Verifies that the pipeline handles an empty input stream correctly, ensuring it returns an empty result list.
Test pipeline without operations: Ensures a ValueError is raised when attempting to run a pipeline without any operations added.
Test running the pipeline multiple times: Checks that a ValueError is raised if the pipeline is run more than once, to prevent re-use without reinitialization.
Test duplicate operation names: Tests that adding multiple operations with the same name raises a ValueError to prevent operation name conflicts.
Test adding operations after pipeline run: Verifies that a ValueError is raised if attempting to add new operations after the pipeline has already been run.
Test map function with incorrect signature: Ensures a ValueError is raised if a map function with the wrong number of parameters is added.
Test reduce function with incorrect signature: Checks that a ValueError is raised if a reduce function with the wrong number of parameters is added.
Test map function not callable: Verifies that adding a non-callable object as a map function raises a ValueError.
Test reduce function not callable: Confirms that adding a non-callable object as a reduce function raises a ValueError.
Test map function throwing an error: Ensures the pipeline can handle and log errors thrown by map functions, and it verifies the type of result when an error occurs.
Test reduce function throwing an error: Tests the pipeline's ability to handle and log errors thrown by reduce functions, checking the type of result when an error is thrown.
"""


#
# .run()
#

def test_num_processes_invalid():
    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)
    with pytest.raises(ValueError):
        list(pipeline.run(iter([1, 2, 3]), 0))


def test_empty_input_stream():
    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)
    result = list(pipeline.run(iter([]), 1))
    assert result == []


def test_pipeline_without_operations():
    pipeline = Pipeline()
    with pytest.raises(ValueError):
        list(pipeline.run(iter([1, 2, 3]), 1))


def test_run_multiple_times():
    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)
    list(pipeline.run(iter([1, 2, 3]), 1))
    with pytest.raises(ValueError):
        list(pipeline.run(iter([4, 5, 6]), 1))


def test_more_processes_than_items():
    num_items = 4
    num_proc = num_items * 3

    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)

    input_stream = list(range(num_items))

    res = list(pipeline.run(input_stream, num_processes=num_proc))

    assert set(res) == {0, 2, 4, 6}


#
# Building pipelines
#


def test_duplicate_operation_names():
    pipeline = Pipeline()
    pipeline.map('operation', lambda x: x)
    with pytest.raises(ValueError):
        pipeline.map('operation', lambda x: x * 2)


def test_adding_operations_post_run():
    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)
    list(pipeline.run(iter([1, 2, 3]), 1))

    with pytest.raises(ValueError):
        pipeline.map('late', lambda x: x)

    with pytest.raises(ValueError):
        pipeline.reduce('late', lambda x, y: (x, None), {})


#
# Invalid map or reduce functions
#

def test_invalid_function_map():
    pipeline = Pipeline()
    with pytest.raises(ValueError):
        pipeline.map('invalid', 'not_a_function')


def test_invalid_function_reduce():
    pipeline = Pipeline()
    with pytest.raises(ValueError):
        pipeline.reduce('invalid', 'not_a_function', None)


def test_incorrect_parameter_count_map():
    pipeline = Pipeline()
    with pytest.raises(ValueError):
        pipeline.map('complex', lambda x, y: x + y)


def test_incorrect_parameter_count_reduce():
    pipeline = Pipeline()
    with pytest.raises(ValueError):
        pipeline.reduce('complex_reduce', lambda x, y, z: x + y, set())


def test_incompatible_parameters_reduce():
    pipeline = Pipeline()

    def reduce_fn(item: int, aggregate: set) -> tuple:
        aggregate.add(item)
        return item, lambda agg: agg

    # This is ok:
    pipeline.reduce('ok_reduce_step', reduce_fn, initializer=set())

    with pytest.raises(ValueError):
        # Here the initializer's type doesn't match the reduce_fn's aggregate arg type
        pipeline.reduce('ok_reduce_step', reduce_fn, initializer=[])


#
# Checking whether operations (signatures of op fns) logically follow each other
#

def test_map_function_parameter_type_order_in_ops():
    pipeline = Pipeline()

    def correct_map_fn(item: int) -> int:
        return item * 2

    def incorrect_map_fn(item: str) -> int:
        return len(item)

    pipeline.map('correct_map_fn', correct_map_fn)

    with pytest.raises(ValueError):
        # We CAN know this before running the fn, because correct_map_fn returns int (from typehint),
        # while incorrect_map_fn receives str, so they're incompatible
        pipeline.map('incorrect_map_fn', incorrect_map_fn)


def test_reduce_function_parameter_type_order_in_ops():
    pipeline = Pipeline()

    def correct_reduce_fn(item: int, aggregate: set):
        aggregate.add(item)
        return item, lambda agg: agg

    def correct_map_fn(item: int) -> int:
        return item * 2

    def incorrect_reduce_fn(item: str, aggregate: list):
        aggregate.append(item)
        return len(item), lambda agg: agg.append(item)

    pipeline.reduce('correct_reduce_fn', correct_reduce_fn, set())
    pipeline.map('correct_map_fn', correct_map_fn)

    with pytest.raises(ValueError):
        # correct_map_fn returns int, incorrect_reduce_fn expects str
        pipeline.reduce('incorrect_reduce_fn', incorrect_reduce_fn, set())


#
# Error handling inside map / reduce
#

def test_map_function_error():
    def faulty_map(x):
        if x > 1:
            raise ValueError("Test error")
        return x * 2

    pipeline = Pipeline()
    pipeline.map('faulty', faulty_map)
    res = list(pipeline.run(iter([1, 2, 3]), 1))
    assert [type(r) for r in res] == [int, ValueError, ValueError]


def test_reduce_function_error():
    def faulty_reduce(x, agg):
        if x > 1:
            raise ValueError("Test error")
        return (x, lambda agg: agg.add(x))

    pipeline = Pipeline()
    pipeline.reduce('faulty_reduce', faulty_reduce, set())
    res = list(pipeline.run(iter([1, 2, 3]), 1))
    assert [type(r) for r in res] == [int, ValueError, ValueError]


#
# Performance
#


def test_scalability():
    num_tasks = 100
    input_stream = list(range(num_tasks))

    pipeline = Pipeline()
    pipeline.map('scale', lambda x: x * 2)
    results_single = list(pipeline.run(input_stream, 1))

    pipeline2 = Pipeline()
    pipeline2.map('scale', lambda x: x * 2)
    results_multi = list(pipeline2.run(input_stream, 4))

    # FIXME: (just an example, but we want something like this
    # assert time(results_multi) < time(results_single)


#
# Concurrency safety
#

# FIXME: do it with hypothesis. would make much more sense.
# TODO: come up with ways to test thread safety!
@pytest.mark.skip()
def test_concurrency_safety():
    pipeline = Pipeline()
    pipeline.reduce('accumulate', lambda x, y: (x + y, lambda agg: agg + x), 0)
    input_stream = iter(range(10))
    results = list(pipeline.run(input_stream, 5))
    assert sum(results) == sum(range(10))  # Sum might not match due to race conditions if not handled properly


#
# FIXME
#


# TODO: interesting. how should we handle it when there the results are the same value? Here we use a set to remove the
#   duplicate values, but it's an explicit reduce step which is not the best. ofc reduce is good for this, but maybe
#   we should make it part of the framework? e.g. a builtin "unique" reducer
# TODO: caching. how? we need a plan to cache. How is it connected? We don't want to repeat the operations for the
#   same value. every map fn and reduce fn should be a pure, idempotent function, so we can cache every result based
#   on some "key_fn" which generates a small key based on some result. for caching, we don't really care about the race
#   condition where e.g. a result is cached for "key1" but another worker also worker with "key1", but accessed it
#   before "key1" was written. Why? Because we'd rather do some work twice than to wait for some lock! If we want
#   explicit locking, we can use a reducer.
@pytest.mark.skip('FIXME')
def test_state_consistency():
    pipeline = Pipeline()
    pipeline.reduce('unique_only', lambda x, y: (x, lambda agg: agg.union({x})), set())
    input_stream = iter([1, 1, 2, 2, 3, 3])
    results = list(pipeline.run(input_stream, 1))
    assert len(set(results)) == 3
