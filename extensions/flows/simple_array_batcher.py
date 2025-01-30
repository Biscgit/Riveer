import threading
import time

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

    def function(self, data, *args) -> None:
        if isinstance(data, dict):
            data = [data]

        with self._synchronizer:
            self._buffer += data

            if self._is_batching:
                return

            self._is_batching = True

        wait_time = self._config["processing"]["batch_seconds"]
        time.sleep(wait_time)

        with self._synchronizer:
            finished_buffer = self._buffer.copy()
            self._buffer.clear()
            self._is_batching = False

        readers = self._config["processing"]["output"]
        NodeGraph.send_result(finished_buffer, readers)
