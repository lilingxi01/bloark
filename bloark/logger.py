import multiprocessing as mp
import os
import logging
from logging.handlers import QueueListener, QueueHandler
from typing import Union


_log_format = "[%(asctime)s (%(process)d) %(levelname)s] %(message)s"
_formatter = logging.Formatter(_log_format)


def _get_logger_stream_handler(log_level: int) -> logging.StreamHandler:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(_formatter)
    stream_handler.setLevel(log_level)
    return stream_handler


def _get_logger_file_handler(log_name: str, log_dir: str, log_level: int) -> Union[logging.FileHandler, None]:
    if log_dir is not None:
        os.makedirs(log_dir, exist_ok=True)
        log_path = os.path.join(log_dir, f'{log_name}.log')
        file_handler = logging.FileHandler(filename=log_path, mode='a')
        file_handler.setFormatter(_formatter)
        file_handler.setLevel(log_level)
        return file_handler
    return None


def cleanup_logger(log_name: str, log_dir: str):
    if log_dir is not None and os.path.exists(log_dir):
        log_file_path = os.path.join(log_dir, f'{log_name}.log')
        if not os.path.exists(log_file_path):
            return
        try:
            os.remove(log_file_path)
        except Exception as e:
            logging.error(f'Error occurred while cleaning up the log file {log_file_path}: {e}')


def _cleanup_logger_handlers(logger: logging.Logger):
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])


def universal_logger_init(log_name: str, log_dir: str, log_level: int = logging.INFO):
    stream_handler = _get_logger_stream_handler(log_level=log_level)
    file_handler = _get_logger_file_handler(log_name=log_name, log_dir=log_dir, log_level=log_level)

    logger = logging.getLogger()

    # Clean up any existing handlers in default logger.
    _cleanup_logger_handlers(logger)

    logger.setLevel(log_level)
    logger.addHandler(stream_handler)
    if file_handler is not None:
        logger.addHandler(file_handler)


def mp_logger_init(log_name: str, log_dir: str, log_level: int = logging.INFO) -> (QueueListener, mp.Queue):
    q = mp.Queue()

    stream_handler = _get_logger_stream_handler(log_level=log_level)
    file_handler = _get_logger_file_handler(log_name=log_name, log_dir=log_dir, log_level=log_level)

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


def mp_child_logger_init(q: mp.Queue, log_level: int = logging.INFO):
    qh = QueueHandler(q)
    logger = logging.getLogger()

    # Clean up any existing handlers in subprocess default logger.
    _cleanup_logger_handlers(logger)

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
