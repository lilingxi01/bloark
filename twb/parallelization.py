import logging
import os.path
import shutil
from typing import Union, Callable, TypeVar, Tuple, Any, Generic
from multiprocessing import Manager, Lock, Value, Pool


# ========== Global Variables for cross-process communication ==========


global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map


def _init_worker(inner_parallel_lock, inner_logger_lock, inner_curr_index, inner_curr_count, pid_map):
    global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map
    _parallel_lock = inner_parallel_lock
    _logger_lock = inner_logger_lock
    _curr_index = inner_curr_index
    _curr_count = inner_curr_count
    _pid_map = pid_map


# ========== RDS Process Controller ==========


class RDSProcessController:
    def __init__(self,
                 parallel_lock: Lock,
                 logger_lock: Lock,
                 curr_index: Value,
                 curr_count: Value,
                 pid_map: dict,
                 log_dir: Union[str, None],
                 log_level: int = logging.DEBUG):
        self.parallel_lock = parallel_lock
        self.logger_lock = logger_lock
        self.curr_index = curr_index
        self.curr_count = curr_count
        self.pid_map = pid_map

        log_handlers = [logging.StreamHandler()]

        if log_dir is not None:
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, 'process.log')
            log_handlers.append(logging.FileHandler(filename=log_path, mode='a'))

        # TODO: Might want to use logger instance instead of logging.basicConfig.
        logging.basicConfig(
            level=log_level,
            format='[%(asctime)s (%(process)d) %(levelname)s] %(message)s',
            handlers=log_handlers
        )
        self.logdebug(f'Process initialized.')

    def declare_index(self):
        self.parallel_lock.acquire()
        index = self.curr_index.value
        self.curr_index.value += 1
        self.parallel_lock.release()
        return index

    def count_forward(self, count: int = 1) -> int:
        self.parallel_lock.acquire()
        self.curr_count.value += count
        curr_final_count = self.curr_count.value
        self.parallel_lock.release()
        return curr_final_count

    def register(self, temporary_dir: Union[str, None] = None):
        """
        Register the process along with its temporary directory path.
        If the process ID already exists, the previous temporary directory will be deleted in case of disk space safety.
        :param temporary_dir: the temporary directory path
        """
        pid = os.getpid()
        if pid in self.pid_map:
            # Cleanup the temporary directory.
            prev_temporary_dir = self.pid_map[pid]
            if prev_temporary_dir and os.path.exists(prev_temporary_dir):
                # Record the existence of the temporary directory, which should be an error.
                self.logerr(f'Temporary directory ({prev_temporary_dir}) is undeleted!')
                shutil.rmtree(prev_temporary_dir)
        self.pid_map[pid] = temporary_dir

    def print(self, *message: Any, severity: str = 'info'):
        """
        Print the message with the process ID.
        :param message: the message to be printed
        :param severity: the severity of the message (info, warning, error)
        """
        self.logger_lock.acquire()
        printable_content = ' '.join([str(m) for m in message])
        if severity == 'warning':
            logging.warning(printable_content)
        elif severity == 'error':
            logging.error(printable_content)
        elif severity == 'fatal':
            logging.critical(printable_content)
        elif severity == 'info':
            logging.info(printable_content)
        else:
            logging.debug(printable_content)
        self.logger_lock.release()

    def logdebug(self, *message: Any):
        self.print(*message, severity='debug')

    def loginfo(self, *message: Any):
        self.print(*message, severity='info')

    def logwarn(self, *message: Any):
        self.print(*message, severity='warning')

    def logerr(self, *message: Any):
        self.print(*message, severity='error')

    def logfatal(self, *message: Any):
        self.print(*message, severity='fatal')


# ========== Inner Executable ==========


# Generic type for the callable function.
_R = TypeVar("_R")
RDSProcessExecutable = Callable[..., _R]  # TODO: Add wild args typing (not supported in Python 3.8).


def _inner_executable(executable: RDSProcessExecutable,
                      add_controller: bool,
                      log_dir: Union[str, None],
                      log_level: int,
                      *inner_args):
    if add_controller:
        global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map
        controller = RDSProcessController(
            parallel_lock=_parallel_lock,
            logger_lock=_logger_lock,
            curr_index=_curr_index,
            curr_count=_curr_count,
            pid_map=_pid_map,
            log_dir=log_dir,
            log_level=log_level
        )
        return executable(controller, *inner_args)  # Inject the controller.
    return executable(*inner_args)


# ========== RDS Process Manager ==========


class RDSProcessManager(Generic[_R]):
    """
    Restrictive Disk Space Process Manager (RDS-PM).
    Manage the processes based on the disk space.

    Deprecation: disk space limitation feature is sunset in v0.2.0 for the sake of simplicity and process security.
    """
    def __init__(self,
                 num_proc: Union[int, None] = 1,
                 log_dir: Union[str, None] = None,
                 log_level: int = logging.DEBUG,
                 start_index: int = 0):
        """
        :param num_proc: the number of processes to be used (default: 1) (None: use all available processes)
        :param log_dir: the dir to the log file (default: None)
        :param log_level: the log level (default: logging.DEBUG)
        :param start_index: the start index of the process
        """
        self.num_proc = num_proc
        self.log_dir = log_dir
        self.log_level = log_level

        # For process security, we will use only 1 process if num_proc is None.
        final_num_proc = num_proc if num_proc is not None else (os.cpu_count() or 1)

        if log_dir is not None and os.path.exists(log_dir):
            shutil.rmtree(log_dir)

        manager = Manager()
        curr_index = manager.Value('i', start_index)
        curr_count = manager.Value('i', 0)
        pid_map = manager.dict()

        parallel_lock = Lock()
        logger_lock = Lock()

        self.pool = Pool(
            processes=final_num_proc,
            initializer=_init_worker,
            initargs=(parallel_lock, logger_lock, curr_index, curr_count, pid_map)
        )

    def apply_async(self,
                    executable: RDSProcessExecutable[_R],
                    args: Tuple[Any, ...],  # Typing of this variable might not be correct.
                    use_controller: bool = True,
                    callback: Union[Callable[[_R], None], None] = None,
                    error_callback: Union[Callable[[Exception], None], None] = None):
        """
        Preserve the given space.
        :param executable: the executable function
        :param args: the arguments to be passed to the executable function
        :param use_controller: whether to add the controller to the arguments
        :param callback: the callback function to be called after the executable function is finished.
        :param error_callback: the callback function to be called if an error occurs.
        """

        # Apply the async function.
        self.pool.apply_async(
            func=_inner_executable,
            args=(executable, use_controller, self.log_dir, self.log_level, *args),
            callback=callback,
            error_callback=error_callback
        )

    def close(self):
        """
        [RDS-PM] Close the pool for further tasks.
        """
        self.pool.close()

    def join(self):
        """
        [RDS-PM] Wait for all the tasks to be finished.
        """
        self.pool.join()

    def terminate(self):
        """
        [RDS-PM] Terminate the pool.
        """
        self.pool.terminate()
