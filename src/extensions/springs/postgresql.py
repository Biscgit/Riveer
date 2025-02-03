import json
import logging
import typing

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool
from voluptuous import Schema, All, Length, Coerce, Optional

from core.app import EnvStr
from core.node import Spring
from core.cron import CronTask


class PostgreSQL(Spring):
    def __init__(self, config):
        super().__init__(config)

        self._connection: ThreadedConnectionPool | None = None

    @staticmethod
    def config_schema() -> "Schema":
        return Schema(
            {
                "connection": {
                    "dbname": EnvStr(),
                    "user": EnvStr(),
                    Optional("password"): EnvStr(),
                    Optional("host"): EnvStr(),
                    Optional("port"): Coerce(int),
                    Optional("minconn", default=1): Coerce(int),
                    Optional("maxconn", default=64): Coerce(int),
                },
                "tasks": [
                    Schema(
                        {
                            "name": str,
                            "cron": str,
                            "query": str,
                            "outputs": All(
                                [str],
                                Length(min=1, msg="At least one output must be defined!"),
                            ),
                            Optional("timeout", default=60): Coerce(int),
                            Optional("fields"): [str],
                        }
                    )
                ],
            }
        )

    def connect(self) -> None:
        logging.info("Connecting to PostgreSQL database")
        self._connection = ThreadedConnectionPool(**self._config["connection"])

    def get_periodic_tasks(self) -> typing.Generator["CronTask"]:
        for config in self._config["tasks"]:
            yield CronTask(
                source=self,
                task_name=config["name"],
                task_args=[config["query"], config["timeout"]],
                task_schedule=config["cron"],
                task_outputs=config["outputs"],
            )

    def function(self, data, *args):
        query, timeout_seconds = data, args[0]
        conn = self._connection.getconn()
        try:
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            cursor.execute(f"SET statement_timeout = {timeout_seconds * 1000}")
            cursor.execute(query)

            rows = cursor.fetchall()
            json_results = json.loads(json.dumps(rows, default=str))

            return json_results

        finally:
            self._connection.putconn(conn)

    def shutdown(self) -> None:
        if self._connection is not None:
            self._connection.closeall()
        logging.info("Closed all PostgreSQL connections for source %s.", self.name)
