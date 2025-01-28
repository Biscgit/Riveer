import logging
import typing

from core.io import AbstractIO
from core.validator import ValidationError

from consumers.sink import AbstractSink
from consumers.source import AbstractSource

logger = logging.getLogger("TaskPipeline")

__all__ = ["PipelineNodes", "send_result"]


class PipelineNodes:
    _input_config_map: dict[str, AbstractSource] = {}
    _transform_config_map: dict[str, AbstractIO] = {}
    _output_config_map: dict[str, AbstractSink] = {}

    _pipe_name_mapping: dict[str, AbstractIO] = {}

    @staticmethod
    def add_new_extensions(cls: type[AbstractIO], overwrite: bool = False):
        def base_add_new_extension(destination: dict):
            name = cls.id()

            if name in destination and not overwrite:
                raise ValueError(f"Node with name `{name}` already exists!")

            destination[name] = cls

        if issubclass(cls, AbstractSource):
            base_add_new_extension(PipelineNodes._input_config_map)

        elif False:
            base_add_new_extension(PipelineNodes._transform_config_map)

        elif issubclass(cls, AbstractSink):
            base_add_new_extension(PipelineNodes._output_config_map)

        else:
            raise RuntimeError("Received class which is not a valid Node!")

    @staticmethod
    def get_node_class(pipe: str, pipe_type: str):
        try:
            if pipe == "input":
                return PipelineNodes._input_config_map[pipe_type]
            elif pipe == "output":
                return PipelineNodes._output_config_map[pipe_type]
            else:
                raise ValidationError(f"Pipe `{pipe}` is unknown and invalid.")

        except KeyError:
            raise ValidationError(f"Pipe `{pipe_type}` is unknown and not supported!")

    @staticmethod
    def insert_abstract_io(name: str, io: AbstractIO):
        if name in PipelineNodes._pipe_name_mapping:
            raise ValidationError(f"Input with name `{name}` already exists!")

        PipelineNodes._pipe_name_mapping[name] = io

    @staticmethod
    def _iter_over_nodes_cls(cls):
        for name, node in PipelineNodes._pipe_name_mapping.items():
            if isinstance(node, cls):
                yield name, node

    @staticmethod
    def iter_input_nodes() -> typing.Generator[typing.Tuple[str, "AbstractSource"], None, None]:
        yield from PipelineNodes._iter_over_nodes_cls(AbstractSource)

    @staticmethod
    def iter_output_nodes() -> typing.Generator[typing.Tuple[str, "AbstractSink"], None, None]:
        yield from PipelineNodes._iter_over_nodes_cls(AbstractSink)

    @staticmethod
    def iter_io_nodes() -> typing.Generator[typing.Tuple[str, "AbstractIO"], None, None]:
        yield from PipelineNodes._iter_over_nodes_cls(AbstractIO)

    @staticmethod
    def send_result(data: list[dict], consumers: list[str]):
        for name in consumers:
            node = PipelineNodes._pipe_name_mapping[name]

            if not isinstance(node, AbstractSink):
                raise RuntimeError(f"Node `{name}` is not a consumer node!")

            node.process.delay(data)


send_result = PipelineNodes.send_result

# class SyncInputPipe:
#     def __init__(self, name: str):
#         self.name = name
#         self._registered_tasks = []
#
#     def add_data(self, data: AbstractResult) -> None:
#         for task in self._registered_tasks:
#             task.delay(data)
#
#     def register_consumer(self, func: Task) -> None:
#         celery_task = celery_app.task(func)
#         self._registered_tasks.append(celery_task)

# # implement redis as a persistent buffer for pipes
# class SynchronizedComponentPipe:
#     def __init__(self, name: str):
#         self.name = name
#         self._list: list[PipeData] = []
#         self._takers: dict[str, int] = {}
#         # self
#
#     def read_stream(self, consumer_name: str):
#         ...
#
#     def write(self, data: PipeData):
#         self._list.append(data)
#         self.notify_consumers()
#
#     def notify_consumers(self):
#         ...
#
#     def take_first(self, taker_name: str) -> typing.Optional[PipeData]:
#         index = self._takers[taker_name]
#         if index < len(self._list):
#             return None
#
#         last_item = self._list[index]
#         self._takers[taker_name] += 1
#
#         smallest_index = min(self._takers.values())
#         if index == 0 and smallest_index > 0:
#             self._takers = {k: v - smallest_index for k, v in self._takers.items()}
#             self._list = self._list[smallest_index:]
#
#         return last_item
#
#     def add_taker(self, taker_name: str):
#         if taker_name in self._takers:
#             raise ValueError(f"Taker with name `{taker_name}` already exists!")
#
#         self._takers[taker_name] = 0
#
#     def __iter__(self):
#         ...
