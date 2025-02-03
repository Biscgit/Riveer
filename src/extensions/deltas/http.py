import json
import logging
import requests
from requests.auth import HTTPBasicAuth

from voluptuous import Schema, Coerce, Optional, Any

from core.app import EnvStr, LowerVal
from core.node import Delta


class BasicHTTP(Delta):
    def __init__(self, config):
        super().__init__(config)

        self._session = requests.Session()

    @staticmethod
    def config_schema() -> "Schema":
        return Schema(
            {
                "connection": {
                    "endpoint": EnvStr(),
                    Optional("auth", default=None): {
                        "username": EnvStr(),
                        "password": EnvStr(),
                    },
                    Optional("method", default="post"): LowerVal(
                        Any("get", "post", "put", "delete")
                    ),
                    Optional("headers", default={}): dict,
                    Optional("allowed_responses", default=[200]): [int],
                    Optional("ping_on_start", default=False): Coerce(bool),
                    Optional("allowed_ping_responses", default=[200]): [int],
                },
                "processing": {
                    "payload_format": Any("json"),
                    Optional("timeout", default=60): Coerce(int),
                },
            }
        )

    def connect(self) -> None:
        config = self._config["connection"]

        if auth := config["auth"]:
            self._session.auth = HTTPBasicAuth(auth["username"], auth["password"])

        if headers := config["headers"]:
            self._session.headers.update(headers)

        if config["ping_on_start"]:
            ...

    def function(self, data: list, *args) -> None:
        payload = json.dumps(data)

        conn_conf = self._config["connection"]
        proc_conf = self._config["processing"]

        with self._session as session:
            response = session.request(
                conn_conf["method"],
                conn_conf["endpoint"],
                data=payload,
                timeout=proc_conf["timeout"],
            )

            if response.status_code not in conn_conf["allowed_responses"]:
                logging.error(
                    "HTTP request failed with status code %s: %s",
                    response.status_code,
                    response.text,
                )

                response.status_code = max(400, response.status_code)
                response.raise_for_status()

    def shutdown(self) -> None:
        self._session.close()
        logging.info("Closed HTTP sessions for delta %s.", self.name)
