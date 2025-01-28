import logging

from celery import current_app as celery_app
from celery.schedules import crontab

from consumers import AbstractSource
from core.pipe import send_result

__all__ = ["SourceTask"]

logger = logging.getLogger("TaskExecution")


class SourceTask:
    def __init__(self, source: AbstractSource, name: str, cron: str, function_args: list | tuple, outputs: list):
        self.schedule = self._load_cron(cron)
        self.source = source
        self.func_args = function_args
        self.name = name
        self.task_consumers = outputs

    @staticmethod
    def _load_cron(cron: str):
        parts = cron.split(" ")[:5]
        parts = {i: v for i, v in enumerate(parts)}

        schedule = {
            "minute": parts.get(0, "*"),
            "hour": parts.get(1, "*"),
            "day_of_week": parts.get(2, "*"),
            "day_of_month": parts.get(3, "*"),
            "month_of_year": parts.get(4, "*"),
        }
        return crontab(**schedule)

    def schedule_task(self):
        name = f"{self.source.type}-{self.name}-process"
        logger.info(f"Loading task: `{name}`")

        celery_task = celery_app.task(
            self.task_wrapper(self.source.task),
            name=name,
            bind=True,
        )
        celery_app.add_periodic_task(
            schedule=self.schedule,
            name=name,
            sig=celery_task.s(*self.func_args),
        )

    def task_wrapper(self, func):
        def inner(task, *args):
            try:
                result = func(*args)
                if not isinstance(result, list):
                    raise ValueError("Source input must be a list of values")

                send_result(
                    result,
                    consumers=self.task_consumers
                )

            except Exception as e:
                logging.error(f"{e.__class__.__name__} | Task {task.name} failed to execute because: {e}")

        return inner
