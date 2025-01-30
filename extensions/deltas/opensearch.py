from itertools import chain
import logging

from opensearchpy import OpenSearch as OpenSearchConn

from core.node import Delta


class OpenSearch(Delta):
    def __init__(self, config):
        self._connection: OpenSearchConn | None = None
        super().__init__(config)

    @classmethod
    def id(cls) -> str:
        return "opensearch"

    @classmethod
    def from_configuration(cls, config: dict) -> "OpenSearch":
        return cls(config)

    # @staticmethod
    # def connection_schema() -> dict:
    #     return {
    #         "host": Required(str),
    #         "port": Required(int),
    #         "user": Required(str),
    #         "password": Required(str),
    #         "url_prefix": Optional(str, ""),
    #         "use_ssl": Optional(bool, True),
    #         "verify_certs": Optional(bool, True),
    #         "ca_cert_path": Optional(str, None),
    #         "pool_maxsize": Optional(int, 16)
    #     }

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
        insert_index = {
            "index": {
                "_index": self._config["processing"]["index"]}}
        payload = list(chain.from_iterable(
            [(insert_index, val) for val in data]))
        self._connection.bulk(payload)

    def shutdown(self) -> None:
        if self._connection is not None:
            self._connection.close()
        logging.info("Closed all OpenSearch connections for sink %s.", self.name)
