import inspect
import logging
from contextlib import contextmanager
from multiprocessing import Manager, Process

# FIXME: remove dep

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class Context:
    def __init__(self):
        self.manager = Manager()
        self.locks = {}
        self.data = self.manager.dict({})
        self.output_queue = self.manager.Queue()

    def add_context_key(self, context_key, initializer):
        if context_key in self.data:
            raise KeyError(f"Context key '{context_key}' already exists")

        self.data[context_key] = initializer
        self.locks[context_key] = self.manager.Lock()

    # @contextmanager
    # # def get_state(self, context_key):
    # #     with self.locks[context_key]:
    # #         yield self.data[context_key], lambda fn: self.data.__setitem__(context_key, fn(self.data[context_key]))

    @contextmanager
    def get_state(self, context_key):
        with self.locks[context_key]:
            aggregate = self.data[context_key]

            def set_val(set_fn):
                updated_val = set_fn(aggregate)
                self.data[context_key] = updated_val
                return updated_val

            yield aggregate, set_val

    def yield_result(self, value):
        self.output_queue.put(value)


class Pipeline:
    def __init__(self):
        self.operations = []
        self.context = Context()
        self.ran = False

    def map(self, name, func):
        self._validate_signature(func, 1)
        self._validate_unique_name(name)
        self._validate_not_ran()

        self.operations.append(('map', name, func))
        return self

    def reduce(self, name, func, initializer):
        self._validate_signature(func, 2)
        self._validate_unique_name(name)
        self._validate_not_ran()

        self.operations.append(('reduce', name, func,))
        self.context.add_context_key(name, initializer)
        return self

    def run_for_item(self, item):
        result = item
        for op in self.operations:
            op_type, op_name, op_func = op[0], op[1], op[2]
            logger.debug(f'[{op_type}] {op_name}] {str(result)}')
            try:
                if op_type == 'map':
                    result = op_func(result)
                if op_type == 'reduce':
                    with self.context.get_state(op_name) as (aggregate, set_val):
                        result, setter = op_func(result, aggregate)
                        if setter:
                            set_val(setter)
            except Exception as e:
                logger.error(f"Error in {op_type} operation '{op_name}': {e}", exc_info=True)
                result = ValueError(f'[{op_type}] {op_name} - {str(e)}')
                break

            if result is None:
                return

        self.context.yield_result(result)

    def run(self, input_stream, num_processes):
        self._validate_not_ran()
        if num_processes < 1:
            raise ValueError("Number of processes must be at least 1")
        if not self.operations:
            raise ValueError("No operations have been added to the pipeline")

        self.ran = True
        processes = []
        try:
            for batch in _batch_generator(input_stream, num_processes):
                processes.clear()

                for item in batch:
                    process = Process(target=self.run_for_item, args=(item,))
                    processes.append(process)
                    process.start()

                for process in processes:
                    process.join()

                output_queue = self.context.output_queue
                while not output_queue.empty():
                    yield output_queue.get()

        finally:
            # Ensure all remaining processes are properly terminated
            for process in processes:
                if process.is_alive():
                    process.terminate()
                process.join()

    def _validate_not_ran(self):
        if self.ran:
            raise ValueError('Pipeline has already ran')

    def _validate_unique_name(self, name):
        if any(name == op_name for _, op_name, _ in self.operations):
            raise ValueError(f"Operation name '{name}' must be unique")

    def _validate_signature(self, func, expected_params_count):
        """
        Validates that the function has the correct number of parameters, and if `prev_func` is provided,
        checks that the output type of `prev_func` matches the input type of `func`.
        """
        if not callable(func):
            raise ValueError(f"Expected a callable function, got {type(func).__name__}")

        sig = inspect.signature(func)
        parameters = list(sig.parameters.values())

        if len(parameters) != expected_params_count:
            raise ValueError(
                f"Function {func.__name__} expects {expected_params_count} parameters, got {len(parameters)}")

        if self.operations:
            prev_func = self.operations[-1][2]
            prev_output = inspect.signature(prev_func).return_annotation

            current_input = parameters[0].annotation if parameters else None
            if current_input is not inspect.Signature.empty and prev_output is not inspect.Signature.empty:
                if not issubclass(prev_output, current_input):
                    raise ValueError(
                            f"Type mismatch: {prev_func.__name__} returns {prev_output} but {func.__name__} expects {current_input}")


def _batch_generator(input_stream, batch_count):
    """Yields batches of the specified size from the input stream."""
    batch = []
    for item in input_stream:
        batch.append(item)
        if len(batch) == batch_count:
            yield batch
            batch = []
    if batch:  # Yield any remaining items as the last batch
        yield batch
