import atexit
import logging
import os

from core.extensions import get_ext_classes
from core.validator import ValidationError, validate_and_process_schema, Required, Optional
import yaml

from core.pipe import PipelineNodes
from consumers import AbstractSink, AbstractSource

from celery import current_app as celery_app

logger = logging.getLogger("TaskController")


class TaskController:
    class Globals:
        inputs: dict[str, AbstractSource] = {}
        outputs: dict[str, AbstractSink] = {}

    def setup(self) -> None:
        try:
            logger.info("Loading extensions...")
            self._load_extensions()
            print(list(PipelineNodes.iter_io_nodes()))

            logger.info("Building streams from configuration...")
            self._load_configurations()
            self._establish_outside_connections()

        except Exception as e:
            text = f"Encountered unrecoverable {e.__class__.__name__} during setup!"
            logger.critical(f"SETUP FAILURE!\n{'- ' * (len(text) // 2)}\n{text}\n{e}\nExiting...")
            exit(1)

        logger.info("Successfully build streams")

    def start(self) -> None:
        logger.info("Creating tasks from configuration...")
        self._register_processing_inputs()
        self._create_source_tasks()

        logger.info("Registering shutdown tasks...")
        atexit.register(self.shutdown_all)

    @staticmethod
    def _load_extensions():
        for extension in get_ext_classes():
            PipelineNodes.add_new_extensions(extension)

    def _load_configurations(self, folder: str = None):
        folder = folder or os.getenv("CONFIG_FOLDER", "./configs")
        logger.info(f"Loading configurations from folder {folder}")

        for file_name in os.listdir(folder):
            file_path = os.path.join(folder, file_name)

            with open(file_path, "r") as f:
                base_config: dict = yaml.safe_load(f)

                configuration_schema = {
                    "configuration": {
                        "pipe": Required(str),
                        "type": Required(str),
                        "name": Optional(str, file_name.rsplit(".")[0]),
                    },
                    "connection": Required(dict),
                    "tasks": Optional(dict, {}),
                }
                base_config = validate_and_process_schema(configuration_schema, base_config, [])

                try:
                    pipe = base_config["configuration"]["pipe"].lower()

                    if pipe in ["input", "output"]:
                        self._load_io_config(base_config)

                    else:
                        raise ValidationError(f"Pipe `{pipe}` is not set or not supported!")

                except ValidationError as e:
                    logger.critical("Failed to validate source configurations!")
                    logger.critical(f"Error occurred in file `{file_path}`.")
                    raise e

                else:
                    logger.info(f"Loaded configuration `{file_path}`")

    @staticmethod
    def _load_io_config(base_config: dict):
        file_config = base_config["configuration"]

        io_type = file_config["pipe"]
        cls = PipelineNodes.get_node_class(io_type, file_config["type"])

        connection_schema = cls.connection_schema()
        base_config["connection"] = validate_and_process_schema(
            connection_schema, base_config["connection"], ["connection"]
        )

        if io_type == "input":
            task_schema = cls.task_schema()
            base_config["tasks"] = validate_and_process_schema(task_schema, base_config["tasks"], ["tasks"])

        PipelineNodes.insert_abstract_io(file_config["name"], cls.from_configuration(base_config))

    @staticmethod
    def _establish_outside_connections():
        for name, source in PipelineNodes.iter_io_nodes():
            source.connect()
            logging.info(f"`{source.name}` successfully connected to its source")

        for name, source in PipelineNodes.iter_io_nodes():
            source.check_connection()
            logging.info(f"`{source.name}` successfully passed the health check")

    @staticmethod
    def _register_processing_inputs():
        for name, source in PipelineNodes.iter_output_nodes():
            input_task = celery_app.task(
                source.process,
                name=f"{source.type}-{name}-process",
                bind=True,
            )
            source.process = input_task

    @staticmethod
    def _create_source_tasks():
        for name, source in PipelineNodes.iter_input_nodes():
            tasks = list(source.create_tasks())

            for task in tasks:
                task.schedule_task()
                logging.info(f"Task `{task.name}` successfully scheduled")

    @staticmethod
    def shutdown_all():
        logger.info("Running shutdown tasks...")
        for _, source in PipelineNodes.iter_io_nodes():
            source.shutdown()
