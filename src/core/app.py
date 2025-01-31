import atexit
import logging
import os

import yaml
from voluptuous import Schema, All, Coerce, Optional, Any

from core.graph import NodeGraph
from core.modules import Modules
from core.node import Spring, Flow, Delta

LowerVal = lambda *t: All(Coerce(lambda s: str(s).lower()), *t)
EnvStr = lambda *t: All(Coerce(lambda s: os.path.expandvars(str(s))), *t)


class AppController:
    """This is the central controller of the app."""

    @staticmethod
    def load():
        """Loads all available modules into the app."""
        logging.info("Initializing modules.")
        Modules.initialize()

    def configure(self) -> None:
        """Loads the configurations into the app and initializes tasks."""
        logging.info("Loading configurations.")
        self._load_configurations()

        logging.info("Registering cleanup task")
        atexit.register(self._shutdown)

        logging.info("Creating and validating node tasks")
        self._create_node_tasks()

        logging.info("Establishing node connections")
        self._establish_connections()

    def _load_configurations(self):
        folder = os.getenv("RIVEER_CONFIG", "./configs")

        for file_name in os.listdir(folder):
            file_path = os.path.join(folder, file_name)

            with open(file_path, "r", encoding="utf-8") as f:
                base_config: dict = yaml.safe_load(f)
                default_name = file_name.rsplit(".")[0]

                validated_config = self.get_header_schema(default_name)(base_config)
                self._load_node(validated_config)

    @staticmethod
    def get_header_schema(default_name: str) -> "Schema":
        """This holds the `configuration` schema for the header of the config."""
        return Schema(
            {
                "configuration": {
                    "pipe": LowerVal(Any("spring", "flow", "delta")),
                    "type": LowerVal(),
                    Optional("name", default=default_name): LowerVal(str),
                },
                Any(str): Any(dict, list, str, int),
            }
        )

    @staticmethod
    def _load_node(base_config: dict) -> None:
        """Creates a Node from config."""
        file_config = base_config["configuration"]

        pipe_type = file_config["pipe"]
        cls = Modules.get_node_cls(pipe_type, file_config["type"])

        NodeGraph.register_node(
            file_config["name"], cls.from_configuration(base_config)
        )

    @staticmethod
    def _establish_connections():
        """Runs `connect` for each IO-Node."""
        for name, node in NodeGraph.iter_over_nodes(Spring, Delta):
            try:
                node.connect()
            except Exception as e:
                logging.error("Spring `%s` failed to connect to its source", name)
                raise e

    @staticmethod
    def _shutdown():
        """Runs cleanup function on app shutdown."""
        for _, node in NodeGraph.iter_over_nodes(Spring, Delta):
            node.shutdown()

    @staticmethod
    def _create_node_tasks():
        """Creates periodic tasks defined for Nodes."""
        for name, node in NodeGraph.iter_over_nodes(Spring, Flow):
            for task in node.get_periodic_tasks():
                try:
                    task.schedule_task_function()
                except Exception as e:
                    logging.error(
                        "Failed to create task `%s` from node `%s`.", task.name, name
                    )
                    raise e
