# orc

multiprocessing data pipeline orchestrator

> Experimental! Work in progress.

This package helps you to orchestrate compute-heavy data transformations using multiprocessing, offloading the work to separate Python processes.

## Usage

```python
pipeline = (Pipeline()
            .map(double)
            .reduce(accumulate, lambda: 0)
            .map(lambda x: x+1))

# create an infinite stream of numbers: 0, 1, 2...
stream = itertools.count()

# process results, 12 at a time
for item in pipeline.run(input_stream=stream, num_processes=12):
    print(item)
```

## Authors

```
All Rights Reserved
Copyright (c) 2024 Mice Pápai
```
