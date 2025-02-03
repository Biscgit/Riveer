import logging
import typing

from core.graph import NodeGraph

logger = logging.getLogger("NodeTask")


def _task_wrapper(func: typing.Callable, output_ids: list[str]) -> typing.Callable:
    """This wraps the function to send the result to the next node."""

    def inner(task, task_data, *args) -> None:
        try:
            logger.info("Running Spring task %s", task.name)

            result = func(task_data, *args)

            if result is not None:
                NodeGraph.send_result(result, output_ids)

        except Exception as e:
            logger.error(
                "[%s] Task %s failed to execute because: %s",
                *(e.__class__.__name__, task.name, str(e)),
            )

    return inner


TaskWrapper = _task_wrapper
