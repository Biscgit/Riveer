import logging
import typing

from celery.schedules import crontab
from celery import current_app as celery_app

from core.graph import NodeGraph
from core.node import PipeWriter, PipeReader

logger = logging.getLogger("Tasks")


class CronTask:
    def __init__(
        self,
        source: "PipeWriter",
        task_name: str,
        task_schedule: str,
        task_args: list | tuple,
        task_outputs: list[str],
    ):
        self._source = source
        self.name = f"{source.node_type()}-{source.name}-{task_name}-schedule"
        self._schedule = self._parse_cron(task_schedule)
        self._task_args = task_args
        self._output_ids = task_outputs

        for output_id in self._output_ids:
            self.check_pipeline(output_id, [])

    def check_pipeline(self, node_id: str, stack: list[str]) -> None:
        """Checks if all specified nodes exist for connection."""

        node = NodeGraph.get(node_id)

        if node_id in stack:
            logging.error("Detected closed loop in node `%s` sending to itself.", node_id)
        elif node is None:
            logging.error("Node `%s` cannot accept data to because is does not exist.", node_id)
        elif not isinstance(node, PipeReader):
            logging.error("Node `%s` is Spring and cannot accept pipeline inputs.", node_id)
        else:
            if not node.output_ids:
                logger.warning("Node `%s` can write but has no output nodes defined.", node_id)

            for output_id in node.output_ids:
                self.check_pipeline(output_id, stack + [node_id])
            return

        logger.error(
            "Error from spring `%s` with pipeline stack: [ %s ]",
            self._source.name,
            " -> ".join(stack + [node_id]),
        )
        raise ValueError("Invalid pipeline configuration.")

    @staticmethod
    def _parse_cron(cron_str: str) -> crontab:
        """Returns a celery crontab object from a cron string."""
        parts = dict(enumerate(cron_str.split(" ")))

        fields = ["minute", "hour", "day_of_month", "month_of_year", "day_of_week"]
        return crontab(**{name: parts.get(i, "*") for i, name in enumerate(fields)})

    def schedule_task_function(self):
        """Schedules a new task from the config of this object."""
        name = f"{self._source.node_type()}-{self._source.name}-{self.name}-schedule"

        task = celery_app.task(
            TaskWrapper(self._source.function, self._output_ids),
            name=name,
            bind=True,
        )
        celery_app.add_periodic_task(
            sig=task.s(*self._task_args),
            name=name,
            schedule=self._schedule,
        )


# ToDo: point to invalid or undefined fields on validation!


# ToDo: make this function more general and apply to all tasks being run!
def _task_wrapper(func: typing.Callable, output_ids: list[str]) -> typing.Callable:
    """This wraps the function to send the result to the next node."""

    def inner(task, *args) -> None:
        try:
            logger.info("Running Spring task `%s`", task.name)

            result = func(*args)
            NodeGraph.send_result(result, output_ids)

        except Exception as e:
            logger.error(
                "%s | Task %s failed to execute because: %s",
                *(e.__class__.__name__, task.name, str(e)),
            )

    return inner


TaskWrapper = _task_wrapper
