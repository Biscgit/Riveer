import typing

if typing.TYPE_CHECKING:
    from core.node import Delta, Flow, Spring

    type Data = list | dict
    type NodeType = typing.Union[Spring, Flow, Delta]
    type NodeNameGenerator = typing.Generator[tuple[str, NodeType]]


class NodeGraph:
    """This holds the names of each Node that can be triggered."""

    _pipe_name_mapping: dict[str, "NodeType"] = {}

    @classmethod
    def get(cls, node_name: str) -> typing.Optional["NodeType"]:
        """Returns the node object for the provided name."""
        return cls._pipe_name_mapping.get(node_name)

    @classmethod
    def send_result(cls, data: "Data", readers: list[str]) -> None:
        """Send a data object to many consumers."""
        for reader in readers:
            node = cls.get(reader)
            node.function.delay(data)

    @classmethod
    def register_node(cls, name: str, node: "NodeType"):
        """Adds a new node object to the graph."""
        if name in cls._pipe_name_mapping:
            raise ValueError(f"Node of name `{name}` already exists.")

        cls._pipe_name_mapping[name] = node

    @classmethod
    def iter_over_nodes(cls, *filter_cls: type["NodeType"]) -> "NodeNameGenerator":
        """Yields every node filtering by the provided classes if any."""
        for name, node in cls._pipe_name_mapping.items():
            if not filter_cls or isinstance(node, filter_cls):
                yield name, node
