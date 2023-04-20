import logging
import os.path
from typing import Union, Callable, TypeVar, Tuple, Any, Generic
from multiprocessing import Manager, Lock, Value, Pool

from .logger import mp_logger_init, mp_child_logger_init, twb_logger
from .utils import cleanup_dir


# ========== Global Variables for cross-process communication ==========


global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map, _active_pids


def _init_worker(q, inner_parallel_lock, inner_logger_lock, inner_curr_index, inner_curr_count, pid_map, active_pids,
                 log_level):
    global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map, _active_pids
    _parallel_lock = inner_parallel_lock
    _logger_lock = inner_logger_lock
    _curr_index = inner_curr_index
    _curr_count = inner_curr_count
    _pid_map = pid_map
    _active_pids = active_pids

    # Initialize the logger within the sub-process.
    mp_child_logger_init(q, log_level=log_level)

    # Log the initialization of the process.
    twb_logger.debug('Process initialized.')


# ========== RDS Process Controller ==========


class RDSProcessController:
    def __init__(self,
                 parallel_lock: Lock,
                 logger_lock: Lock,
                 curr_index: Value,
                 curr_count: Value,
                 pid_map: dict,
                 active_pids: list,
                 num_proc: int):
        self.parallel_lock = parallel_lock
        self.logger_lock = logger_lock
        self.curr_index = curr_index
        self.curr_count = curr_count
        self.pid_map = pid_map
        self.active_pids = active_pids
        self.num_proc = num_proc

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
        try:
            pid = os.getpid()
            if pid in self.pid_map:
                # Cleanup the temporary directory.
                prev_temporary_dir = self.pid_map[pid]
                if prev_temporary_dir and os.path.exists(prev_temporary_dir):
                    # Record the existence of the temporary directory, which should be an error.
                    self.logerr(f'Temporary directory ({prev_temporary_dir}) for current pid is undeleted!')
                    cleanup_dir(prev_temporary_dir)
            self.pid_map[pid] = temporary_dir
        except Exception as e:
            self.logfatal(f'Failed to register the process: {e}')
            return

    def unregister(self):
        """
        Unregister the process.
        """
        try:
            temp_dir = None
            pid = os.getpid()
            with self.parallel_lock:
                if pid in self.pid_map:
                    temp_dir = self.pid_map[pid]
                    del self.pid_map[pid]
                if pid in self.active_pids:
                    self.active_pids.remove(pid)
        except Exception as e:
            self.logfatal(f'Failed to unregister the process: {e}')
            return

        try:
            # Cleanup the temporary directory if it exists.
            if temp_dir is not None and os.path.exists(temp_dir):
                cleanup_dir(temp_dir)
        except Exception as e:
            self.logfatal(f'Failed to clean up temporary directory: {e}')

    def print(self, *message: Any, severity: int = logging.DEBUG):
        """
        Print the message with the process ID.
        :param message: the message to be printed
        :param severity: the severity of the message (info, warning, error)
        """
        # In current version, we are logging without logger lock to avoid the deadlock process.
        twb_logger.log(*message, severity=severity)

    def logdebug(self, *message: Any):
        self.print(*message, severity=logging.DEBUG)

    def loginfo(self, *message: Any):
        self.print(*message, severity=logging.INFO)

    def logwarn(self, *message: Any):
        self.print(*message, severity=logging.WARNING)

    def logerr(self, *message: Any):
        self.print(*message, severity=logging.ERROR)

    def logfatal(self, *message: Any):
        self.print(*message, severity=logging.CRITICAL)


# ========== Inner Executable ==========


# Generic type for the callable function.
_R = TypeVar("_R")
RDSProcessExecutable = Callable[..., _R]  # TODO: Add wild args typing (not supported in Python 3.8).


def _inner_executable(executable: RDSProcessExecutable,
                      add_controller: bool,
                      num_proc: int,
                      *inner_args):
    if add_controller:
        global _parallel_lock, _logger_lock, _curr_index, _curr_count, _pid_map, _active_pids
        controller = RDSProcessController(
            parallel_lock=_parallel_lock,
            logger_lock=_logger_lock,
            curr_index=_curr_index,
            curr_count=_curr_count,
            pid_map=_pid_map,
            active_pids=_active_pids,
            num_proc=num_proc
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
                 log_name: str,
                 log_dir: Union[str, None] = None,
                 log_level: int = logging.DEBUG,
                 num_proc: Union[int, None] = 1,
                 start_index: int = 0):
        """
        :param log_name: the name of the log file
        :param log_dir: the dir to the log file (default: None)
        :param log_level: the log level (default: DEBUG)
        :param num_proc: the number of processes to be used (default: 1) (None: use all available processes)
        :param start_index: the start index of the process
        """
        self.num_proc = num_proc
        self.log_dir = log_dir
        self.log_level = log_level

        # For process security, we will use only 1 process if num_proc is None.
        final_num_proc = num_proc if num_proc is not None else (os.cpu_count() or 1)

        ql, q = mp_logger_init(log_name=log_name, log_dir=log_dir, log_level=log_level)
        self.queue_listener = ql  # Will need to stop it in the end.

        manager = Manager()
        curr_index = manager.Value('i', start_index)
        curr_count = manager.Value('i', 0)
        pid_map = manager.dict()
        active_pids = manager.list()

        parallel_lock = Lock()
        logger_lock = Lock()

        self.manager = manager

        self.pool = Pool(
            processes=final_num_proc,
            initializer=_init_worker,
            initargs=(q, parallel_lock, logger_lock, curr_index, curr_count, pid_map, active_pids, log_level)
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
            args=(executable, use_controller, self.num_proc, *args),
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
        Will terminate the problematic pool after all the tasks are done.
        """
        self.pool.join()
        self.queue_listener.stop()

    def terminate(self):
        """
        [RDS-PM] Terminate the pool.
        """
        self.pool.terminate()
