import os
from queue import Empty
from typing import Union, Callable
from multiprocessing import Manager, Lock, Value, Queue, Process, cpu_count
import signal


class RDSProcessController:
    def __init__(self,
                 space: int,
                 parallel_lock: Lock,
                 logger_lock: Lock,
                 curr_index: Value,
                 registered_space: Value,
                 total_space: Union[int, None]):
        self.space = space
        self.parallel_lock = parallel_lock
        self.logger_lock = logger_lock
        self.curr_index = curr_index
        self.registered_space = registered_space
        self.total_space = total_space

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

    def release(self):
        if self.total_space is not None:
            self.parallel_lock.acquire()
            self.registered_space.value -= self.space
            self.parallel_lock.release()


RDSProcessExecutable = Callable[[str, str, RDSProcessController, dict], None]


class RDSProcessManager:
    """
    Restrictive Disk Space Process Manager (RDS-PM). Manage the processes based on the disk space.
    """
    def __init__(self,
                 executable: RDSProcessExecutable,
                 total_space: Union[int, None] = None,
                 num_proc: Union[int, None] = None,
                 context: Union[None, dict] = None,
                 start_index: int = 0):
        """
        :param executable: the executable function
        :param total_space: the total space to be used (default: the total space of the disk)
        :param num_proc: the number of processes to be used (default: the number of CPUs)
        :param context: the context to be passed to the executable function
        :param start_index: the start index of the process
        """
        self.executable = executable
        self.total_space = total_space
        self.num_proc = num_proc
        self.context = context

        self.manager = Manager()
        self.curr_index = self.manager.Value('i', start_index)

        self.initial_queue = []

    def register(self, path: str, output_dir: str, space: int):
        """
        Preserve the given space.
        :param path: the path to be preserved
        :param output_dir: the output directory
        :param space: the space to be preserved
        """
        if not os.path.exists(path):
            raise FileNotFoundError('The path does not exist.')

        # Make sure path is not in the queue.
        # TODO: There might be a better way to do this. E.g. using a set instead of a list. But this is fine for now.
        for i in range(len(self.initial_queue)):
            if self.initial_queue[i][0] == path:
                del self.initial_queue[i]
                break

        if space <= 0:
            print(f'[RDS-PM] The space is not positive. Skip {path}.')
            return

        self.initial_queue.append((path, output_dir, space))

    def start(self):
        # Initialize locks.
        parallel_lock = Lock()
        logger_lock = Lock()

        # Initialize the registered space, a process-safe variable.
        registered_space = self.manager.Value('i', 0)

        queue = Queue()
        for config in self.initial_queue:
            queue.put(config)

        # Determine the number of processes.
        max_allowed_proc = cpu_count()
        specified_num_proc = self.num_proc if self.num_proc is not None else max_allowed_proc
        selected_num_proc = min(max_allowed_proc, specified_num_proc)

        # Start process.
        process_pool = []
        for _ in range(selected_num_proc):
            process = Process(
                target=process_consumer,
                args=(queue, registered_space, self.curr_index, logger_lock, parallel_lock,
                      self.executable, self.total_space, self.context)
            )
            process.start()
            process_pool.append(process)

        # Wait for all processes to finish.
        for process in process_pool:
            process.join()
            curr_exitcode = process.exitcode
            if curr_exitcode == -9:
                signal_name = 'Killed - probably out of memory'
            elif curr_exitcode < 0:
                signal_name = signal.Signals(abs(curr_exitcode)).name
            else:
                signal_name = str(curr_exitcode)
            print(f'[RDS-PM] Process {process.pid} closing. ({signal_name})')
            process.close()


def process_consumer(queue: Queue,
                     registered_space: Value,
                     curr_index: Value,
                     logger_lock: Lock,
                     parallel_lock: Lock,
                     executable: RDSProcessExecutable,
                     total_space: Union[int, None],
                     context: Union[None, dict] = None):
    while True:
        try:
            config = queue.get_nowait()
        except Empty:
            break

        path, output_dir, space = config

        if total_space is not None:
            if space > total_space:
                print(f'[RDS-PM] The space is larger than the total space. Skip {path}.')
                continue

            parallel_lock.acquire()
            available_space = total_space - registered_space.value
            if available_space < space:
                queue.put(config)
                parallel_lock.release()
                continue
            registered_space.value += space
            parallel_lock.release()

        # Execute the executable along with the process controller.
        controller = RDSProcessController(
            space=space,
            parallel_lock=parallel_lock,
            logger_lock=logger_lock,
            curr_index=curr_index,
            registered_space=registered_space,
            total_space=total_space
        )
        executable(path, output_dir, controller, context)

        if total_space is not None:
            parallel_lock.acquire()
            registered_space.value -= space
            parallel_lock.release()
