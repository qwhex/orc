import itertools
from contextlib import contextmanager
from multiprocessing import Manager, Process
from time import sleep

from toolz import take


class Context:
    def __init__(self):
        self.manager = Manager()
        self.data = self.manager.dict({})
        self.output_queue = self.manager.Queue()

    def add_context_key(self, context_key, initializer):
        self.data[context_key] = initializer

    @contextmanager
    def get_state(self, context_key):
        with self.manager.Lock():
            def get_val():
                return self.data[context_key]

            def set_val(set_fn):
                updated_val = set_fn(self.data[context_key])
                self.data[context_key] = updated_val
                return updated_val

            try:
                yield get_val, set_val
            finally:
                pass

    def yield_result(self, value):
        self.output_queue.put(value)


class Pipeline:
    def __init__(self):
        self.operations = []
        self.context = Context()
        self.ran = False

    def map(self, name, func):
        self.operations.append(('map', name, func))
        return self

    def reduce(self, name, func, initializer):
        self.operations.append(('reduce', name, func,))
        self.context.add_context_key(name, initializer)
        return self

    def run_for_item(self, item):
        result = item
        for op in self.operations:
            op_type, op_name, op_func = op[0], op[1], op[2]

            if op_type == 'map':
                result = op_func(result)
            if op_type == 'reduce':
                with self.context.get_state(op_name) as state:
                    result = op_func(result, state)

            if result is None:
                return None

        self.context.yield_result(result)

    def run(self, input_stream, num_processes):
        if self.ran:
            raise ValueError('Pipeline has already run')

        try:
            processes = []
            while True:
                items = list(take(num_processes, input_stream))
                if not items:
                    break

                for item in items:
                    process = Process(target=self.run_for_item, args=(item,))
                    processes.append(process)
                    process.start()

                for process in processes:
                    process.join()

                output_queue = self.context.output_queue
                while not output_queue.empty():
                    yield output_queue.get()

                processes.clear()
        finally:
            self.ran = True


def double(x):
    return x * 2


def only_one_for_tenths(x, state):
    get_val, set_val = state
    sleep(0.1)

    tenth = x // 10
    if tenth in get_val():
        return None
    else:
        set_val(lambda prev: prev.union({tenth}))
        return x


def main():
    pipeline = (Pipeline()
                .map(name='double', func=double)
                .reduce(name='only_one_for_tenths',
                        func=only_one_for_tenths,
                        initializer=set()))

    for item in pipeline.run(input_stream=itertools.count(), num_processes=4):
        print(item)


main()