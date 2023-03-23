import multiprocessing as mp
import os
import logging
from logging.handlers import QueueListener, QueueHandler
from typing import Union


_log_format = "[%(asctime)s (%(process)d) %(levelname)s] %(message)s"
_formatter = logging.Formatter(_log_format)


def _get_logger_stream_handler() -> logging.StreamHandler:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(_formatter)
    return stream_handler


def _get_logger_file_handler(log_dir: str) -> Union[logging.FileHandler, None]:
    if log_dir is not None:
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, 'process.log')
        file_handler = logging.FileHandler(filename=log_path, mode='a')
        file_handler.setFormatter(_formatter)
        return file_handler
    return None


def universal_logger_init(log_dir: str, log_level: int = logging.DEBUG):
    stream_handler = _get_logger_stream_handler()
    file_handler = _get_logger_file_handler(log_dir=log_dir)

    logger = logging.getLogger()

    # Clean up any existing handlers in default logger.
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    logger.setLevel(log_level)
    logger.addHandler(stream_handler)
    if file_handler is not None:
        logger.addHandler(file_handler)


def mp_logger_init(log_dir: str, log_level: int = logging.DEBUG) -> (QueueListener, mp.Queue):
    q = mp.Queue()

    stream_handler = _get_logger_stream_handler()
    file_handler = _get_logger_file_handler(log_dir=log_dir)

    if file_handler is not None:
        ql = QueueListener(q, stream_handler, file_handler)
    else:
        ql = QueueListener(q, stream_handler)
    ql.start()

    logger = logging.getLogger('mp_parent_logger')  # Use a different name to avoid conflict with the default logger.
    logger.setLevel(log_level)

    logger.addHandler(stream_handler)
    if file_handler is not None:
        logger.addHandler(file_handler)

    return ql, q


def mp_child_logger_init(q: mp.Queue, log_level: int = logging.DEBUG):
    qh = QueueHandler(q)
    logger = logging.getLogger()

    # Clean up any existing handlers in subprocess default logger.
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    logger.setLevel(log_level)
    logger.addHandler(qh)


class _TWBLogger:
    @staticmethod
    def log(*message, severity: int = logging.DEBUG):
        printable_content = ' '.join([str(m) for m in message])
        logging.log(severity, printable_content)

    @staticmethod
    def info(*message):
        _TWBLogger.log(*message, severity=logging.INFO)

    @staticmethod
    def debug(*message):
        _TWBLogger.log(*message, severity=logging.DEBUG)

    @staticmethod
    def warning(*message):
        _TWBLogger.log(*message, severity=logging.WARNING)

    @staticmethod
    def error(*message):
        _TWBLogger.log(*message, severity=logging.ERROR)

    @staticmethod
    def critical(*message):
        _TWBLogger.log(*message, severity=logging.CRITICAL)


twb_logger = _TWBLogger
