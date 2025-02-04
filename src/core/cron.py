import logging

from celery.schedules import crontab
from celery import current_app as celery_app

from core.graph import NodeGraph
from core.node import GraphWriter, GraphReader
from core.task import TaskWrapper

logger = logging.getLogger("CronTask")


class CronTask:
    def __init__(
        self,
        source: "GraphWriter",
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
            self.check_pipeline(output_id, [f"{source.name}/{task_name}"])

    def check_pipeline(self, node_id: str, stack: list[str]) -> None:
        """Checks if all specified nodes exist for connection."""

        node = NodeGraph.get(node_id)

        if node_id in stack:
            logging.error("Detected closed loop in node `%s` sending to itself.", node_id)
        elif node is None:
            logging.error("Node `%s` cannot accept data to because is does not exist.", node_id)
        elif not isinstance(node, GraphReader):
            logging.error("Node `%s` is Spring and cannot accept pipeline inputs.", node_id)
        else:
            if not node.output_ids:
                logger.warning("Node `%s` can write but has no output nodes defined.", node_id)

            for output_id in node.output_ids:
                self.check_pipeline(output_id, stack + [node_id])
            return

        logger.error(
            "Error from spring `%s` with pipeline stack: [ %s ]",
            *(self._source.name, " -> ".join(stack + [node_id])),
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
        task = celery_app.task(
            TaskWrapper(self._source.function, self._output_ids),
            name=self.name,
            bind=True,
        )
        celery_app.add_periodic_task(
            sig=task.s(*self._task_args),
            name=self.name,
            schedule=self._schedule,
        )

# ToDo: point to invalid or undefined fields on validation!
# ToDo: proper task timeouts
