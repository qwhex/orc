import itertools

import pytest
from hypothesis import strategies as st, given

from orc.pipeline import Pipeline
from tests.test_pipeline import (slow_double, slow_tenth,
                                 only_one_for_tenths)

# Correctly assign functions to operation types
map_functions = [slow_double, slow_tenth]
reduce_functions = [only_one_for_tenths]

# Define a combined list of operations with their designated types
combined_operations = [('map', fn) for fn in map_functions] + [('reduce', fn) for fn in reduce_functions]

# Operations strategy
operation_strategy = st.lists(
        st.sampled_from(combined_operations),
        min_size=1,
        max_size=7
).map(lambda ops: [(i, op[0], op[1]) for i, op in enumerate(ops)])  # Adding unique suffix using index

# Input stream strategies
input_stream_strategy = st.one_of(
        st.just(itertools.count()),  # Infinite generator
        st.lists(st.integers(), min_size=1, max_size=1000)  # Finite list with random integers
)

num_processes_strategy = st.integers(min_value=1, max_value=24)
num_results_to_take_strategy = st.integers(min_value=1, max_value=1000)


@given(input_stream=input_stream_strategy, num_processes=num_processes_strategy,
       num_results_to_take=num_results_to_take_strategy, operations=operation_strategy)
@pytest.mark.skip('foo')
def test_pipeline_operations(input_stream, num_processes, num_results_to_take, operations):
    pipeline = Pipeline()
    for idx, op_type, op_func in operations:
        unique_name = f"{op_type}_{op_func.__name__}_{idx}"
        if op_type == 'map':
            pipeline.map(name=unique_name, func=op_func)
        elif op_type == 'reduce':
            pipeline.reduce(name=unique_name, func=op_func, initializer=set())

    # Collect results
    results = list(
            itertools.islice(pipeline.run(input_stream=input_stream, num_processes=num_processes), num_results_to_take))
    # Ensure no exceptions in results
    assert all(not isinstance(result, Exception) for result in results)


# Example usage
# test_pipeline_operations()
