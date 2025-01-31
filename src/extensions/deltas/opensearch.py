import logging

from opensearchpy import OpenSearch as OpenSearchConn
from opensearchpy.helpers import parallel_bulk
from voluptuous import Schema, Coerce, Optional

from core.app import EnvStr
from core.node import Delta


class OpenSearch(Delta):
    def __init__(self, config):
        self._connection: OpenSearchConn | None = None
        super().__init__(config)

    @staticmethod
    def config_schema() -> "Schema":
        return Schema(
            {
                "connection": {
                    "host": EnvStr(),
                    "port": EnvStr(Coerce(int)),
                    "user": EnvStr(),
                    "password": EnvStr(),
                    Optional("url_prefix", default=None): EnvStr(),
                    Optional("ca_cert_path", default="/etc/ssl/certs/"): EnvStr(),
                    Optional("use_ssl", default=True): Coerce(bool),
                    Optional("verify_certs", default=True): Coerce(bool),
                    Optional("http_compress", default=True): Coerce(bool),
                },
                "processing": {
                    "index": EnvStr(),
                    Optional("timeout", default=60): EnvStr(Coerce(int)),
                },
            }
        )

    def connect(self) -> None:
        logging.info("Connecting to OpenSearch database")

        conn_conf = self._config["connection"]
        self._connection = OpenSearchConn(
            hosts=[{"host": conn_conf["host"], "port": conn_conf["port"]}],
            http_compress=True,
            http_auth=(conn_conf["user"], conn_conf["password"]),
            use_ssl=conn_conf["use_ssl"],
            verify_certs=conn_conf["verify_certs"],
            ca_certs=conn_conf["ca_cert_path"],
            url_prefix=conn_conf["url_prefix"],
        )

        self._connection.ping()
        # self._connection.indices.create(self._config["processing"]["index"])

    def function(self, data: list[dict], *args) -> None:
        proc_conf = self._config["processing"]
        payload = [d | {"_index": proc_conf["index"]} for d in data]

        parallel_bulk(self._connection, payload, request_timeout=proc_conf["timeout"])

    def shutdown(self) -> None:
        if self._connection is not None:
            self._connection.close()
        logging.info("Closed all OpenSearch connections for delta %s.", self.name)
