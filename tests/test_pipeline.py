import itertools
import random
import time
from itertools import islice
from time import sleep

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

# List of test cases:
"""
Test Cases for the Pipeline's .run() Method:
Test Invalid Number of Processes: Ensures the pipeline raises a ValueError if attempted to run with zero processes, validating the requirement for at least one process.
Test Empty Input Stream: Confirms that the pipeline handles an empty input stream gracefully by returning an empty list of results.
Test Pipeline Without Operations: Checks that attempting to run a pipeline without any operations defined raises a ValueError.
Test Running the Pipeline Multiple Times: Verifies that the pipeline raises a ValueError if there is an attempt to run it more than once, ensuring that pipelines are not reused without reinitialization.
Test More Processes Than Items: Ensures that the pipeline can handle cases where the number of processes exceeds the number of items, and that it processes the input correctly.
Test Cases for Building and Configuring the Pipeline:
Test Duplicate Operation Names: Ensures that adding operations with duplicate names is not allowed and raises a ValueError.
Test Adding Operations After Pipeline Run: Verifies that operations cannot be added after the pipeline has started running, raising a ValueError if this rule is violated.
Test Cases for Validating Map and Reduce Functions:
Test Invalid Map Function: Checks that attempting to add a non-callable object as a map function raises a ValueError.
Test Invalid Reduce Function: Ensures that a non-callable object cannot be added as a reduce function, expecting a ValueError.
Test Incorrect Parameter Count in Map Function: Validates that adding a map function with the wrong number of parameters raises a ValueError.
Test Incorrect Parameter Count in Reduce Function: Tests that a reduce function with an incorrect number of parameters raises a ValueError.
Test Incompatible Parameters in Reduce Function: Confirms that a ValueError is raised when the initializer's type does not match the expected type of the reduce function's aggregation argument.
Test Cases for Ensuring Logical Compatibility Between Operations:
Test Map Function Parameter Type Order: Checks that consecutive map functions have compatible parameter and return types, raising a ValueError for type mismatches.
Test Reduce Function Parameter Type Order: Ensures that the output type of a map function matches the expected input type of a subsequent reduce function, raising a ValueError for any type inconsistency.
Test Cases for Error Handling Within Operations:
Test Map Function Error Handling: Ensures that the pipeline can handle and appropriately log errors thrown by map functions. It checks the types of results to verify error handling.
Test Reduce Function Error Handling: Tests the pipeline's ability to manage and log errors occurring in reduce functions, verifying the types of results to ensure errors are handled correctly.
Performance Testing:
Test Scalability: Measures the execution times for single-threaded versus multi-threaded runs to ensure that increasing the number of processes decreases the execution time, thereby confirming the pipeline's scalability.
Miscellaneous and Skipped Tests:
Test Concurrency Safety (Skipped): Proposed to test the pipeline's thread safety using a property-based testing framework like Hypothesis, but currently skipped.
Tests with Pending Implementation or Clarification:
Test State Consistency (Skipped): A skipped test meant to verify the pipeline's handling of duplicate values using a built-in "unique" reducer. This is marked for future implementation and a fix.
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


def test_pipeline_with_infinite_iterator():
    num_items = 100

    pipeline = Pipeline()
    pipeline.map('double', lambda x: x * 2)

    infinite_input_stream = itertools.count()

    pipeline_gen = pipeline.run(infinite_input_stream, 4)
    results = list(islice(pipeline_gen, num_items))

    assert len(results) == num_items, "The number of results should be exactly 100."


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

    # Measure execution time for single-threaded run
    start_time_single = time.time()
    results_single = list(pipeline.run(input_stream, 1))
    end_time_single = time.time()
    time_single = end_time_single - start_time_single

    pipeline2 = Pipeline()
    pipeline2.map('scale', lambda x: x * 2)

    # Measure execution time for multi-threaded run
    start_time_multi = time.time()
    results_multi = list(pipeline2.run(input_stream, 4))
    end_time_multi = time.time()
    time_multi = end_time_multi - start_time_multi

    # Verify results are the same
    assert set(results_single) == set(results_multi), "Multi-threaded results do not match single-threaded results."

    # Check if multi-threaded execution is faster
    assert time_multi < time_single, f"Multi-threaded run ({time_multi}s) should be faster than single-threaded run ({time_single}s)."

    # Optionally, log or print the time difference for a clearer comparison
    print(f"Single-threaded execution time: {time_single} seconds")
    print(f"Multi-threaded execution time: {time_multi} seconds")


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
    assert sum(results) == sum(range(10))


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
