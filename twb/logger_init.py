import logging
import multiprocessing as mp
from logging.handlers import QueueListener, QueueHandler

_log_format = "[%(asctime)s (%(process)d) %(levelname)s] %(message)s"
_formatter = logging.Formatter(_log_format)


def _get_logger_stream_handler(log_level: int) -> logging.StreamHandler:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(_formatter)
    stream_handler.setLevel(log_level)
    return stream_handler


def _cleanup_logger_handlers(logger: logging.Logger):
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])


def _init_logger_main_process(log_level: int):
    stream_handler = _get_logger_stream_handler(log_level=log_level)
    logger = logging.getLogger()
    _cleanup_logger_handlers(logger)
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)


def _init_logger_multiprocessing(log_level: int = logging.INFO) -> (QueueListener, mp.Queue):
    q = mp.Queue()

    stream_handler = _get_logger_stream_handler(log_level=log_level)

    ql = QueueListener(q, stream_handler)
    ql.start()

    # I use a different name here to avoid conflict with the default logger.
    logger = logging.getLogger('mp_parent_logger')
    logger.setLevel(log_level)
    logger.addHandler(stream_handler)

    return ql, q


def _init_logger_sub_process(q: mp.Queue, log_level: int = logging.INFO):
    qh = QueueHandler(q)
    logger = logging.getLogger()

    # Clean up any existing handlers in subprocess default logger.
    _cleanup_logger_handlers(logger)
    logger.setLevel(log_level)
    logger.addHandler(qh)
