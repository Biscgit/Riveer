import os

from celery import Celery
import logging

from core.app import AppController

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", logging.INFO),
    format='[%(asctime)s: %(levelname)s/%(name)s] %(message)s'
)

app = Celery(
    'metric_collector',
    broker=os.getenv("COLLECTOR_BROKER", 'amqp://guest@localhost:5672//'),
)
app.conf.broker_connection_retry_on_startup = True


@app.on_after_configure.connect
def load_application(**_kwargs):
    tc = AppController()

    try:
        tc.load()
        tc.start()

    except Exception as e:
        logging.critical(f"{e.__class__.__name__}: Failed to setup")
        logging.critical(f"{e}")
        exit(1)

    else:
        logging.info("Application loaded successfully. Starting Celery...")
