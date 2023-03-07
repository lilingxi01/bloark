from typing import Union, Callable, TypeVar, Tuple, Any, Generic
from multiprocessing import Manager, Lock, Value, Pool


class RDSProcessController:
    def __init__(self,
                 parallel_lock: Lock,
                 logger_lock: Lock,
                 curr_index: Value):
        self.parallel_lock = parallel_lock
        self.logger_lock = logger_lock
        self.curr_index = curr_index

    def declare_index(self):
        self.parallel_lock.acquire()
        index = self.curr_index.value
        self.curr_index.value += 1
        self.parallel_lock.release()
        return index

    def print(self, *message: str):
        self.logger_lock.acquire()
        print(*message)
        self.logger_lock.release()


# Generic type for the callable function.
_R = TypeVar("_R")
RDSProcessExecutable = Callable[..., _R]  # TODO: Add wild args typing (not supported in Python 3.8).


global _parallel_lock, _logger_lock, _curr_index


def _init_worker(inner_parallel_lock, inner_logger_lock, inner_curr_index):
    global _parallel_lock, _logger_lock, _curr_index
    _parallel_lock = inner_parallel_lock
    _logger_lock = inner_logger_lock
    _curr_index = inner_curr_index


def _inner_executable(executable: RDSProcessExecutable, add_controller: bool, *inner_args):
    if add_controller:
        global _parallel_lock, _logger_lock, _curr_index
        controller = RDSProcessController(
            parallel_lock=_parallel_lock,
            logger_lock=_logger_lock,
            curr_index=_curr_index
        )
        return executable(controller, *inner_args)  # Inject the controller.
    return executable(*inner_args)


class RDSProcessManager(Generic[_R]):
    """
    Restrictive Disk Space Process Manager (RDS-PM).
    Manage the processes based on the disk space.

    Deprecation: disk space limitation feature is sunset in v0.2.0 for the sake of simplicity and process security.
    """
    def __init__(self,
                 num_proc: Union[int, None] = None,
                 start_index: int = 0):
        """
        :param num_proc: the number of processes to be used (default: the number of CPUs)
        :param start_index: the start index of the process
        """
        self.num_proc = num_proc

        # For process security, we will use only 1 process if num_proc is None.
        final_num_proc = num_proc if num_proc is not None else 1

        manager = Manager()
        curr_index = manager.Value('i', start_index)

        parallel_lock = Lock()
        logger_lock = Lock()

        self.pool = Pool(
            processes=final_num_proc,
            initializer=_init_worker,
            initargs=(parallel_lock, logger_lock, curr_index)
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
            args=(executable, use_controller, *args),
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
