import typing

if typing.TYPE_CHECKING:
    from core.node import Delta, Flow, Spring

    type Data = list | dict
    type NodeType = typing.Union[Spring, Flow, Delta]


class NodeGraph:
    """This holds the names of each Node that can be triggered."""

    _pipe_name_mapping: dict[str, "NodeType"] = {}

    @staticmethod
    def send_result(data: "Data", readers: list[str]) -> None:
        """Send a data object to many consumers."""
        print(f"GOT RESULT to {readers}")
        for reader in readers:
            node = NodeGraph._pipe_name_mapping[reader]
            node.function.delay(data)

    @staticmethod
    def register_node(name: str, node: "NodeType"):
        """Adds a new node object to the graph."""
        if name in NodeGraph._pipe_name_mapping:
            raise ValueError(f"Node of name `{name}` already exists.")

        NodeGraph._pipe_name_mapping[name] = node

    @staticmethod
    def iter_over_nodes(
            *filter_cls: type["NodeType"],
    ) -> typing.Generator[tuple[str, "NodeType"]]:
        """Yields every node filtering by the provided classes if any."""
        for name, node in NodeGraph._pipe_name_mapping.items():
            if not filter_cls or isinstance(node, filter_cls):
                yield name, node
