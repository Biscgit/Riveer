import json
import logging

from psycopg2.extras import RealDictCursor

from core.validator import Optional, Required, AnyName
from consumers import AbstractSource
from consumers.sourcetask import SourceTask

import psycopg2
import psycopg2.pool


class PostgreSQLSource(AbstractSource):

    def __init__(self, config):
        self._config = config
        self._connection: psycopg2.pool.ThreadedConnectionPool | None = None

    @property
    def name(self):
        return self._config["configuration"]["name"]

    @classmethod
    def id(cls) -> str:
        return "postgresql"

    @classmethod
    def from_configuration(cls, config: dict) -> "AbstractSource":
        return cls(config)

    @staticmethod
    def connection_schema():
        return {
            "host": Required(str),
            "port": Required(int),
            "user": Required(str),
            "password": Required(str),
            "database": Required(str),
            "max_connections": Optional(int, 255),
        }

    @staticmethod
    def task_schema():
        return {
            AnyName: {
                "query": Required(str),
                "outputs": Required(list),
                "cron": Optional(str, "*"),
                "name": Optional(str, ""),
                "timeout": Optional(int, 60),
                "fields": Optional(str, None)
            }
        }

    def connect(self) -> None:
        logging.info("Connecting to PostgreSQL database")

        conn_conf = self._config["connection"]
        self._connection = psycopg2.pool.ThreadedConnectionPool(
            dbname=conn_conf["database"],
            user=conn_conf["user"],
            password=conn_conf["password"],
            host=conn_conf["host"],
            port=conn_conf["port"],
            minconn=1,
            maxconn=conn_conf["max_connections"],
        )

    def create_tasks(self) -> list[SourceTask]:
        for name, config in self._config["tasks"].items():
            yield SourceTask(
                source=self,
                name=config["name"] or name,
                cron=config["cron"],
                function_args=(config["query"], config["timeout"]),
                outputs=config["outputs"],
            )

    def task(self, *args):
        query, timeout_seconds = args[0], args[1]
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
        self._connection.closeall()
        logging.info(f"Closed all PostgreSQL connections for source {self.name}.")
