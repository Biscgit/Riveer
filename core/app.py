import logging
import typing
import atexit

from core.graph import NodeGraph
from core.modules import Modules

import os
import yaml

from core.node import Spring, Flow, Delta


class AppController:

    def load(self) -> None:
        logging.info("Initializing modules.")
        Modules.initialize()

        logging.info("Loading configurations.")
        self._load_configurations()

    def start(self):
        logging.info("Establishing connections")
        self._establish_connections()

        logging.info("Registering cleanup task")
        atexit.register(self._shutdown)

        logging.info("Summoning node tasks")
        self._create_node_tasks()

    def _load_configurations(self):
        folder = os.getenv("CONFIG_FOLDER", "./configs")

        for file_name in os.listdir(folder):
            file_path = os.path.join(folder, file_name)

            with open(file_path, "r") as f:
                base_config: dict = yaml.safe_load(f)
                self._load_node(base_config)

    @staticmethod
    def _load_node(base_config: dict) -> None:
        """Creates a Node from config."""
        file_config = base_config["configuration"]

        pipe_type = file_config["pipe"]
        cls = Modules.get_node_cls(pipe_type, file_config["type"])

        NodeGraph.register_node(file_config["name"], cls.from_configuration(base_config))

    @staticmethod
    def _establish_connections():
        """Runs `connect` for each IO-Node."""
        for _, node in NodeGraph.iter_over_nodes(Spring, Delta):
            node.connect()

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
                task.schedule_task_function()
