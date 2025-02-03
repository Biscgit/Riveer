import logging
import os
import sys

from celery import Celery

from core.app import AppController

logging.basicConfig(
    level=logging.getLevelName(os.getenv("LOG_LEVEL", "INFO").upper()),
    format="[%(asctime)s: %(levelname)s/%(name)s] %(message)s",
)

app = Celery(
    "Riveer",
    broker=os.getenv("RIVEER_BROKER", "amqp://guest@localhost:5672//"),
)
app.conf.broker_connection_retry_on_startup = True


@app.on_after_configure.connect
def load_application(**_kwargs):
    tc = AppController()

    try:
        tc.load()
        tc.configure()

    except Exception as e:
        logging.critical("%s: Failed to setup", e.__class__.__name__)
        logging.critical("%s", str(e))
        sys.exit(1)

    else:
        logging.info("Application loaded successfully. Starting Celery...")
