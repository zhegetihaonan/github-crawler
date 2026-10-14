import logging
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
import sys
from common.config import LOG_LEVEL


def setup_logger(name):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    logger.propagate = False
    log_dir = Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)

    log_handler = TimedRotatingFileHandler(
        log_dir / f"{name}.log", when="D", interval=1, backupCount=7, encoding="utf-8"
    )
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] [%(name)s] [%(funcName)s-%(lineno)d] %(message)s")
    log_handler.setFormatter(formatter)
    logger.addHandler(log_handler)
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)
    return logger
