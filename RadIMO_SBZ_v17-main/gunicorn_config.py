
import multiprocessing
import logging
from logging.handlers import RotatingFileHandler
import sys
# Basic configuration
bind = "0.0.0.0:5016"
workers = 1
worker_class = "gevent"
worker_connections = 1000
threads = 1 
timeout = 60
keepalive = 5
preload_app = False
max_requests = 0
max_requests_jitter = 0

# Logging configuration
logfile = "/xxxxx/radimo17/gunicorn.log"
loglevel = "warning"
# Setup logging
logger = logging.getLogger("gunicorn.error")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(logfile, maxBytes=1024 * 1024 * 100, backupCount=3)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(processName)s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
def on_starting(server):
    logger.info("Gunicorn server is starting")
    logger.info(f"Python version: {sys.version}")
def when_ready(server):
    logger.info("Gunicorn server is ready")
    logger.info(f"Listening at: {bind}")
    logger.info(f"Worker class: {worker_class}")
    logger.info(f"Number of workers: {workers}")
def on_exit(server):
    logger.info("Gunicorn server is stopping")
def pre_request(worker, req):
    worker.log.info(f"Handling request: {req.method} {req.path}")
def post_request(worker, req, environ, resp):
    worker.log.info(f"Completed request: {req.method} {req.path} - Status: {resp.status}")