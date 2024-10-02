import inspect
import logging
from inspect import _empty
from multiprocessing import Process
from typing import (Any, Tuple, Callable, TypeVar, Optional, Generic, Iterable, get_type_hints, _GenericAlias,
                    _UnionGenericAlias)

from orc.context import Context
from orc.util import batch_generator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Types:
Item = TypeVar('Item')

Stream = Iterable[Item]
MapFunction = Callable[[Any], Any]

# Note: based on this, you can't change a value type through a reducer. Good idea?
Agg = TypeVar('Agg')
ReducerReturn = Tuple[Optional[Any], Optional[Callable[[Agg], Agg]]]
# ReducerReturn = Optional[Tuple[Any, Callable[[Agg], Agg]]]
ReducerFunction = Callable[[Any, Agg], ReducerReturn[Agg]]


# TODO: rename to Orc
class Pipeline(Generic[Item]):
    """multiprocessing data pipeline"""

    def __init__(self):
        self.operations = []
        self.context = Context()

    def run(self,
            input_stream: Stream,
            num_processes: int
            ) -> Stream:

        if num_processes < 1:
            raise ValueError("Number of processes must be at least 1")
        if not self.operations:
            raise ValueError("No operations have been added to the pipeline")

        self.context.init_data()
        processes = []
        # input_stream can be infinite, so we start tasks in batches
        for batch in batch_generator(input_stream, num_processes):
            processes.clear()

            for item in batch:
                process = Process(target=self._run_for_item, args=(item,))
                process.start()
                processes.append(process)

            for process in processes:
                process.join()

            output_queue = self.context.output_queue
            while not output_queue.empty():
                yield output_queue.get()

    def map(self,
            func: MapFunction,
            name: Optional[str] = None
            ) -> 'Pipeline':

        self._validate_signature(func, 1)

        name = self._find_name('map', func, name)
        self._validate_unique_name(name)

        self.operations.append(('map', name, func))
        return self

    def reduce(self,
               func: ReducerFunction[Agg],
               initializer: Callable[[], Agg],
               name: Optional[str] = None
               ) -> 'Pipeline':

        self._validate_signature(func, 2)
        _validate_agg_type(func, initializer)

        name = self._find_name('reduce', func, name)
        self._validate_unique_name(name)

        self.operations.append(('reduce', name, func,))
        self.context.add_context_key(name, initializer)
        return self

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

    def _find_name(self, op_type: str, func: MapFunction | ReducerFunction, name: Optional[str]) -> str:
        if op_type not in {'map', 'reduce'}:
            raise AssertionError(f"`op_type` for {name} isn't map or reduce: {op_type}")

        if not name:
            name = f"{op_type}_{func.__name__}"

        original_name = name
        suffix = 1
        while any(op_name == name for _, op_name, _ in self.operations):
            name = f"{original_name}__{str(suffix).zfill(2)}"
            suffix += 1

        return name

    def _validate_unique_name(self, name: str):
        if any(name == op_name for _, op_name, _ in self.operations):
            raise AssertionError(f"Operation name '{name}' must be unique")

    def _validate_signature(self, func, expected_param_count: int):
        """
        Validates that the function has the correct number of parameters, and if `prev_func` is provided,
        checks that the output type of `prev_func` matches the input type of `func`.
        Allows functions without explicit class type annotations to be used without error.
        """
        if not callable(func):
            raise ValueError(f"Expected a callable function, got {type(func).__name__}")

        sig = inspect.signature(func)
        func_parameters = list(sig.parameters.values())

        if len(func_parameters) != expected_param_count:
            raise ValueError(
                    f"Param count: {func.__name__} expects {expected_param_count} parameters, got {len(func_parameters)}"
            )

        # Check io order
        if self.operations:
            func_param_type = func_parameters[0].annotation
            previous_operation = self.operations[-1]
            prev_func = previous_operation[2]
            prev_func_return = _get_return_type(prev_func)

            # skip lambdas
            if (prev_func_return is _empty
                    or callable(prev_func) and prev_func.__name__ == "<lambda>"
                    or isinstance(prev_func_return, _GenericAlias)
                    or isinstance(prev_func_return, _UnionGenericAlias)
            ):
                # FIXME: fix reducer return type checking
                return

            if not inspect.isclass(func_param_type):
                raise AssertionError(f'current_param_type not class: '
                                     f'{func.__name__}() agg: {func_param_type}, ')

            if not inspect.isclass(prev_func_return):
                raise AssertionError(f'prev_func_return not class: '
                                     f"prev fn {prev_func.__name__}() returns {prev_func_return} {type(prev_func_return)}")

            if not issubclass(prev_func_return, func_param_type):
                raise ValueError(f"Type incompatibility between steps: "
                                 f"New fn {func.__name__}() expects {func_param_type}, "
                                 f"prev fn {prev_func.__name__}() returns {prev_func_return} ")


def _validate_agg_type(func: ReducerFunction[Agg], initializer: Callable[[], Agg]):
    """
    Validates that the type of the initializer's output matches the expected aggregate type.
    """
    func_agg_type = _get_aggregate_type(func)
    init_return_type = _get_return_type(initializer)

    if not inspect.isclass(func_agg_type):
        raise AssertionError(f'func agg type not class: '
                             f'{func.__name__}() agg: {func_agg_type}, ')
    if not inspect.isclass(init_return_type):
        raise AssertionError(f'init_return_type not class: '
                             f'{initializer.__name__}() -> {init_return_type}')

    if not issubclass(init_return_type, func_agg_type):
        raise ValueError(
                f"Initializer return type mismatch: "
                f"{func.__name__}() agg type: {func_agg_type}, "
                f"{initializer.__name__}() ret type: {init_return_type}")


def _get_aggregate_type(func: ReducerFunction) -> type:
    """
    Extract the expected aggregate type from the reducer function's second parameter.
    We use positional access to handle cases where the function parameters might not be named.
    If no specific type is annotated, 'object' is used as a default to allow any type.
    """
    params = list(inspect.signature(func).parameters.values())
    if len(params) != 2:
        raise AssertionError("Reducer function must take two arguments")

    aggregate_param = params[1]
    return aggregate_param.annotation if aggregate_param.annotation is not inspect.Signature.empty else object


def _get_return_type(callable_obj: Callable) -> type:
    """
    Safely extracts the return type from a callable object, returning 'Any' if the type cannot be determined.
    Includes error handling and logging for robustness.
    """
    if not callable(callable_obj):
        raise AssertionError(f'Not callable')

    return_type = get_type_hints(callable_obj).get('return')
    return return_type
