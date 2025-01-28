import os

from celery import Celery
import logging
from core.controller import TaskController

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s: %(levelname)s/%(name)s] %(message)s'
)

app = Celery(
    'metric_collector',
    broker=os.getenv("COLLECTOR_BROKER", 'amqp://guest@localhost:5672//'),
)

app.conf.broker_connection_retry_on_startup = True


@app.on_after_configure.connect
def load_application(**_kwargs):
    tc = TaskController()

    tc.setup()
    tc.start()

    logging.info("Application loaded successfully. Starting Celery...")
