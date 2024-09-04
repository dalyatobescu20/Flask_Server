
"""Module for defining and managing the web server."""

import logging
import time

from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool, logger
from logging.handlers import RotatingFileHandler

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

# webserver.task_runner.start()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")

webserver.job_counter = 1

logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler('webserver.log', maxBytes=10000, backupCount=10)
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
formatter.converter = time.gmtime
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)
from app import routes
