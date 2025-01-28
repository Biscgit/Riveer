import json
import logging
import time
import typing

from core.validator import Optional, Required
from consumers import AbstractSink

from opensearchpy import OpenSearch, ConnectionPool, Connection
from opensearchpy.helpers import bulk, parallel_bulk

logger = logging.getLogger("OpenSearchSink")


class OpenSearchSink(AbstractSink):

    def __init__(self, config):
        self._config = config
        self._connection: typing.Optional[OpenSearch] = None

    @property
    def name(self):
        return self._config["configuration"]["name"]

    @classmethod
    def id(cls) -> str:
        return "opensearch"

    @classmethod
    def from_configuration(cls, config: dict) -> "AbstractSink":
        return cls(config)

    @staticmethod
    def connection_schema() -> dict:
        return {
            "host": Required(str),
            "port": Required(int),
            "user": Required(str),
            "password": Required(str),
            "url_prefix": Optional(str, ""),
            "use_ssl": Optional(bool, True),
            "verify_certs": Optional(bool, True),
            "ca_cert_path": Optional(str, None),
            "pool_maxsize": Optional(int, 16)
        }

    def connect(self) -> None:
        logging.info("Connecting to OpenSearch database")

        conn_conf = self._config["connection"]

        self._connection = OpenSearch(
            hosts=[{'host': conn_conf["host"], 'port': conn_conf["port"]}],
            http_compress=True,
            http_auth=(conn_conf["user"], conn_conf["password"]),
            use_ssl=conn_conf["use_ssl"],
            verify_certs=conn_conf["verify_certs"],
            ca_certs=conn_conf["ca_cert_path"],
            url_prefix=conn_conf["url_prefix"],
            connection_pool_class_kwargs={"maxsize": conn_conf["pool_maxsize"]}
        )

    def check_connection(self) -> bool:
        return self._connection.ping()

    def process(self, data: list[dict]) -> None:
        insert_index = {"index": {"_index": "opendata-kpidev-metrics"}}
        payload = "\n".join([
            f"{json.dumps(insert_index)}\n{json.dumps(val)}" for val in data
        ])

        parallel_bulk(self._connection, payload)

        # self._connection.bulk(payload)

    def shutdown(self) -> None:
        self._connection.close()
        logging.info(f"Closed all OpenSearch connections for sink {self.name}.")
