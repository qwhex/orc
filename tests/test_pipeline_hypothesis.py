import itertools

import pytest
from hypothesis import strategies as st, given

from orc import Pipeline
from orc.timeout import timeout
from tests.helpers import (tenth, double, accumulate, identity, half_int, init_zero)

# Sane limits
MAX_RESULTS_TO_TAKE = 100
TIMEOUT_SECONDS = 1

# Operations
# For simplicity, only int-based maps and reducers
map_functions = [double, tenth, half_int, identity]
reduce_functions = [accumulate]
combined_operations = [('map', fn) for fn in map_functions] + [('reduce', fn) for fn in reduce_functions]
operation_strategy = st.lists(
        st.sampled_from(combined_operations),
        min_size=1,
        max_size=3
)

# Input stram
input_stream_strategy = st.one_of(
        st.just(itertools.count()),
        # st.just(list(range(100))),
        # st.lists(st.integers(), min_size=1, max_size=10)
)

num_processes_strategy = st.one_of(
        st.just(0),
        st.just(1),
        st.just(2),
        st.just(4),
)


@pytest.mark.no_coverage
@given(operations=operation_strategy,
       input_stream=input_stream_strategy,
       num_processes=num_processes_strategy,
       )
def test_pipeline_operations(input_stream, num_processes, operations):
    pipeline = Pipeline()

    for op_type, op_func in operations:
        if op_type == 'map':
            pipeline.map(op_func)
        elif op_type == 'reduce':
            pipeline.reduce(func=op_func, initializer=init_zero)

    res_gen = pipeline.run(input_stream=input_stream, num_processes=num_processes)

    results = []
    with timeout(TIMEOUT_SECONDS) as check_timeout:
        for item in itertools.islice(res_gen, MAX_RESULTS_TO_TAKE):
            results.append(item)
            check_timeout()

    assert all(not isinstance(result, Exception) for result in results)
