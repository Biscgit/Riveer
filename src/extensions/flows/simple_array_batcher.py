import threading
import time

from voluptuous import Schema, All, Length, Coerce, Optional

from core.graph import NodeGraph
from core.node import Flow


class ArrayBatcher(Flow):
    def __init__(self, config):
        super().__init__(config)

        self._is_batching = False
        self._buffer = []
        self._synchronizer = threading.Lock()

    @classmethod
    def from_configuration(cls, config: dict) -> "ArrayBatcher":
        return cls(config)

    @staticmethod
    def config_schema() -> "Schema":
        return Schema(
            {
                "processing": {
                    "outputs": All(
                        [str],
                        Length(min=1, msg="At least one output must be defined!"),
                    ),
                    Optional("timeframe", default=5): Coerce(int),
                },
            }
        )

    def function(self, data, *args) -> list | None:
        proc_conf = self._config["processing"]

        if isinstance(data, dict):
            data = [data]

        with self._synchronizer:
            self._buffer += data

            if self._is_batching:
                return None

            self._is_batching = True

        time.sleep(proc_conf["timeframe"])

        with self._synchronizer:
            finished_buffer = self._buffer.copy()
            self._buffer.clear()
            self._is_batching = False

        return finished_buffer
