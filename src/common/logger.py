import logging
from logging.handlers import TimedRotatingFileHandler
import sys
from common.config import LOG_LEVEL

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVEL)

    log_handler = TimedRotatingFileHandler(f"logs/{name}.log", when="D", interval=1, backupCount=7,
                                           encoding="utf-8")
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s-%(lineno)d] %(message)s")
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    return logger

# logger = setup_logger()
