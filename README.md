# orc

multiprocessing data pipeline orchestrator

> Experimental! Work in progress.

This package helps you to orchestrate compute-heavy data transformations using multiprocessing, offloading the work to separate Python processes.

## Usage

### Map

`.map(map_fn)` applies `map_fn` on each value in the pipeline.

```python
import itertools
from orc import Pipeline

# initialize pipeline
pipeline = (Pipeline()
            .map(lambda x: x * 2)
            .map(lambda x: x + 1))

# create an infinite stream of numbers: 0, 1, 2...
stream = itertools.count()

# process results, 16 at a time
for item in pipeline.run(input_stream=stream, num_processes=16):
    print(item)
```

### Reduce

`.reduce(reduce_fn, init_fn)` collects all items and applies `reduce_fn` to all.

```python
from orc import Pipeline

def accumulate(x: int, agg: int):
    return x + agg, lambda prev: prev + x

pipeline = (Pipeline()
            .map(lambda x: x * 2)
            .map(lambda x: x + 1)
            .reduce(accumulate, lambda: 0))

```

## Authors

```
All Rights Reserved
Copyright (c) 2024 Mice Pápai
```
