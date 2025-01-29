import typing
import logging

from celery.schedules import crontab
from celery import current_app as celery_app

from core.graph import NodeGraph

if typing.TYPE_CHECKING:
    from core.node import PipeWriter


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
        self._name = task_name
        self._schedule = self._parse_cron(task_schedule)
        self._task_args = task_args
        self._output_ids = task_outputs

    @staticmethod
    def _parse_cron(cron_str: str) -> crontab:
        parts = {i: v for i, v in enumerate(cron_str.split(" "))}

        fields = ["minute", "hour", "day_of_week", "day_of_month", "month_of_year"]
        return crontab(**{name: parts.get(i, "*") for i, name in enumerate(fields)})

    def schedule_task_function(self):
        name = f"{self._source.node_type()}-{self._source.name}-{self._name}-schedule"

        task = celery_app.task(
            self._pipe_integration(self._source.function),
            name=name,
            bind=True,
        )
        celery_app.add_periodic_task(
            sig=task.s(*self._task_args),
            name=name,
            schedule=self._schedule,
        )

    def _pipe_integration(self, func):
        def inner(task, *args) -> None:
            try:
                result = func(*args)
                NodeGraph.send_result(result, self._output_ids)

            except Exception as e:
                logging.error(f"{e.__class__.__name__} | Task {task.name} failed to execute because: {e}")

        return inner
