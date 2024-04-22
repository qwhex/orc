import itertools
import time
from itertools import islice
from typing import TypeVar

import pytest

from orc import Pipeline
from orc.pipeline import ReducerReturn
from tests.helpers import (tenth, double, accumulate, half_int, init_zero, only_one_for_tenths, init_set, to_str,
                           unique_filter)

#
# Test helpers
#

A = TypeVar('A')
B = TypeVar('B')


#
# Pipeline fixtures
#


@pytest.fixture
def pipeline():
    return Pipeline()


@pytest.fixture
def pipeline_double():
    pipeline = Pipeline()
    pipeline.map(double)
    return pipeline


#
# Tests
#


def test_map_operation(pipeline_double):
    assert pipeline_double.operations == [('map', 'map_double', double)]


def test_reduce_operation(pipeline):
    pipeline.reduce(accumulate, init_zero)

    assert pipeline.operations == [('reduce', 'reduce_accumulate', accumulate)]

    list(pipeline.run([1, 1, 2, 3, 5], num_processes=4))

    assert 'reduce_accumulate' in pipeline.context.data
    assert pipeline.context.data['reduce_accumulate'] == 12


def test_pipeline_execution(pipeline):
    pipeline.reduce(only_one_for_tenths, init_set)
    pipeline.map(tenth)

    input_stream = [0, 1, 10, 11, 20, 21]
    result = list(pipeline.run(input_stream, 1))
    expected_result = [0, 1, 2]

    """
    E       AssertionError: assert [0, ValueErro...Type object')] == [0, 1, 2]
E         
E         At index 1 diff: ValueError('[reduce] reduce_only_one_for_tenths - cannot unpack non-iterable NoneType object') != 1
E         Left contains 3 more items, first extra item: ValueError('[reduce] reduce_only_one_for_tenths - cannot unpack non-iterable NoneType object')
"""

    assert result == expected_result


#
# Running pipelines
#


def test_num_processes_invalid(pipeline_double):
    with pytest.raises(ValueError):
        list(pipeline_double.run(iter([1, 2, 3]), 0))


def test_pipeline_without_operations(pipeline):
    with pytest.raises(ValueError):
        list(pipeline.run(iter([1, 2, 3]), 1))


def test_empty_input_stream(pipeline_double):
    result = list(pipeline_double.run(iter([]), 1))
    assert result == []


def test_run_multiple_times(pipeline_double):
    """No error when running multiple times"""
    assert 1 == len(list(pipeline_double.run(iter([1]), 1)))
    assert 2 == len(list(pipeline_double.run(iter([2, 3]), 1)))
    assert 3 == len(list(pipeline_double.run(iter([4, 5, 6]), 1)))


def test_more_processes_than_items(pipeline_double):
    num_items = 4
    num_proc = num_items * 3
    input_stream = list(range(num_items))

    res = list(pipeline_double.run(input_stream, num_processes=num_proc))

    assert set(res) == {0, 2, 4, 6}


def test_pipeline_with_infinite_iterator(pipeline_double):
    num_items = 100

    infinite_input_stream = itertools.count()

    pipeline_gen = pipeline_double.run(infinite_input_stream, 4)
    results = list(islice(pipeline_gen, num_items))

    assert len(results) == num_items, "The number of results should be exactly 100"


#
# Building pipelines
#


def test_duplicate_operation_names(pipeline):
    pipeline.map(double, name='operation')
    pipeline.map(double, name='operation')

    operation_names = {op[1] for op in pipeline.operations}
    assert operation_names == {'operation', 'operation__01'}


def test_lambda_ops(pipeline):
    pipeline.map(lambda x: x * 2, name='double')
    pipeline.map(lambda x: x / 2, name='halve')
    pipeline.map(lambda x: pow(x, 2), name='square')

    input_list = [1, 2, 3]
    res = list(pipeline.run(input_list, 1))
    assert len(input_list) == len(res)


def test_adding_operations_post_run(pipeline_double):
    input_list = [1, 2, 3]
    res = list(pipeline_double.run(input_list, 1))
    assert len(input_list) == len(res)
    assert {2, 4, 6} == set(res)

    # adding another double()
    pipeline_double.map(double)

    res = list(pipeline_double.run(input_list, 1))
    assert len(input_list) == len(res)
    assert {4, 8, 12} == set(res)


#
# Invalid map or reduce functions
#

def test_invalid_function_map(pipeline):
    with pytest.raises(ValueError):
        pipeline.map('not_a_function')


def test_invalid_function_reduce(pipeline):
    with pytest.raises(ValueError):
        pipeline.reduce(None, lambda: None)


def test_incorrect_parameter_count_map(pipeline):
    with pytest.raises(ValueError):
        pipeline.map(lambda x, y: x + y)


def test_incorrect_parameter_count_reduce(pipeline):
    with pytest.raises(ValueError):
        pipeline.reduce(lambda x, y, z: x + y, set)


def test_incompatible_parameters_reduce(pipeline):
    pipeline.reduce(accumulate, initializer=init_zero)

    with pytest.raises(ValueError):
        # Here the initializer's type doesn't match the reduce_fn's aggregate arg type
        pipeline.reduce(accumulate, initializer=init_set)


#
# Checking whether operations (signatures of op fns) logically follow each other
#

def test_map_function_parameter_type_order_in_ops(pipeline):
    pipeline.map(double)
    pipeline.map(to_str)

    with pytest.raises(ValueError):
        # We CAN know that this is wrong before running the fn, because:
        # - to_str returns an str (typehint),
        # - half_int wants to receive an int (typehint),
        # so they're incompatible.
        pipeline.map(half_int)


@pytest.mark.skip('FIXME')
def test_reduce_function_parameter_type_order_in_ops(pipeline):
    pipeline.map(double)
    pipeline.reduce(accumulate, init_zero)

    def incorrect_reduce_fn(item: str, _agg: int) -> ReducerReturn[str]:
        return item, lambda prev: prev

    with pytest.raises(ValueError):
        # correct_map_fn returns int, incorrect_reduce_fn expects str
        pipeline.reduce(incorrect_reduce_fn, init_zero)


#
# Error handling inside map / reduce
#

def test_map_function_error():
    def faulty_map(x):
        if x > 1:
            raise ValueError("Test error")
        return x * 2

    pipeline = Pipeline()
    pipeline.map(faulty_map)
    res = list(pipeline.run(iter([1, 2, 3]), 1))
    assert [type(r) for r in res] == [int, ValueError, ValueError]


def test_reduce_function_error(pipeline):
    def faulty_reduce(x: int, _agg: set) -> ReducerReturn[set]:
        if x > 1:
            raise ValueError("Test error")
        return x, lambda agg: agg.union({x})

    pipeline.reduce(faulty_reduce, init_set)
    res = list(pipeline.run([1, 2], 1))

    assert [type(r) for r in res] == [int, ValueError]


#
# Performance
#


def test_scalability(pipeline_double):
    num_tasks = 100
    input_stream = list(range(num_tasks))

    start_time_single = time.time()
    results_single = list(pipeline_double.run(input_stream, 1))
    time_single = time.time() - start_time_single

    start_time_multi = time.time()
    results_multi = list(pipeline_double.run(input_stream, 4))
    time_multi = time.time() - start_time_multi

    assert set(results_single) == set(results_multi), "Multi-threaded results do not match single-threaded results."
    assert time_multi < time_single, "Multi-threaded run should be faster than single-threaded."

    print(f"Single-threaded execution time: {time_single:.4f} seconds")
    print(f"Multi-threaded execution time: {time_multi:.4f} seconds")


#
# Concurrency safety
#

# FIXME: do it with hypothesis. would make much more sense.
# TODO: come up with ways to test thread safety!
@pytest.mark.skip()
def test_concurrency_safety(pipeline):
    pipeline.reduce(accumulate, init_zero)
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
def test_state_consistency(pipeline):
    pipeline.reduce(unique_filter, set)
    input_stream = iter([1, 1, 2, 2, 3, 3])
    results = list(pipeline.run(input_stream, 1))
    assert len(set(results)) == 3
