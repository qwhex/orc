import inspect
import logging
from multiprocessing import Process
from typing import Any, Tuple, Callable, TypeVar, Optional, Generic, Iterable

from orc.context import Context
from orc.util import batch_generator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Types:
Item = TypeVar('Item')

Stream = Iterable[Item]
MapFunction = Callable[[Any], Any]

# Note: based on this, you can't change a value type through a reducer. Good idea?
ReduceItem = TypeVar('ReduceItem')
Agg = TypeVar('Agg')
ReducerFunction = Callable[[ReduceItem, Agg], Tuple[Optional[ReduceItem], Optional[Callable[[Agg], Agg]]]]


class Pipeline(Generic[Item]):
    def __init__(self):
        self.operations = []
        self.context = Context()
        self.ran = False

    def map(self,
            name: str,
            func: MapFunction,
            ) -> 'Pipeline':

        self._validate_signature(func, 1)
        self._validate_unique_name(name)
        self._validate_not_ran()

        self.operations.append(('map', name, func))
        return self

    def reduce(self,
               name: str,
               func: ReducerFunction[ReduceItem, Agg],
               initializer: Agg
               ) -> 'Pipeline':

        self._validate_signature(func, 2)
        self._validate_agg_type(func, initializer)
        self._validate_unique_name(name)
        self._validate_not_ran()

        self.operations.append(('reduce', name, func,))
        self.context.add_context_key(name, initializer)
        return self

    def run(self,
            input_stream: Stream,
            num_processes: int
            ) -> Stream:

        self._validate_not_ran()
        if num_processes < 1:
            raise ValueError("Number of processes must be at least 1")
        if not self.operations:
            raise ValueError("No operations have been added to the pipeline")

        self.ran = True
        processes = []
        try:
            for batch in batch_generator(input_stream, num_processes):
                processes.clear()

                for item in batch:
                    process = Process(target=self._run_for_item, args=(item,))
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

    def _run_for_item(self, item: Item) -> None:
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
                # TODO: Consider logging the stack trace or re-raising the exception with additional context instead of
                #  converting all errors to ValueError.
                result = ValueError(f'[{op_type}] {op_name} - {str(e)}')
                break

            if result is None:
                return

        self.context.yield_result(result)

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

        # Check args
        if not self.operations:
            return

        prev_func = self.operations[-1][2]
        prev_output = inspect.signature(prev_func).return_annotation

        current_input = parameters[0].annotation if parameters else None
        if current_input is inspect.Signature.empty or prev_output is inspect.Signature.empty:
            return

        if not issubclass(prev_output, current_input):
            raise ValueError(f"Type mismatch: {prev_func.__name__} returns {prev_output} "
                             f"but {func.__name__} expects {current_input}")

    def _validate_agg_type(self, func, initializer):
        expected_agg_type = self._get_aggregate_type(func)
        if not isinstance(initializer, expected_agg_type):
            raise ValueError(
                    f"Initializer type {type(initializer)} does not match expected aggregate type {expected_agg_type}")

    def _get_aggregate_type(self, func: ReducerFunction) -> type:
        """
        Extract the expected aggregate type from the reducer function's second parameter.
        We use positional access to handle cases where the function parameters might not be named.
        If no specific type is annotated, 'object' is used as a default to allow any type.
        """
        sig = inspect.signature(func)
        params = list(sig.parameters.values())
        if len(params) < 2:
            raise ValueError("Reducer function must take at least two arguments")
        aggregate_param = params[1]
        return aggregate_param.annotation if aggregate_param.annotation is not inspect.Signature.empty else object
