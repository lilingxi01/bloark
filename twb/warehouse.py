import logging
import os
from typing import Tuple, Union, List, Dict, Optional, Callable
import multiprocessing as mp


def init_warehouse_kwargs() -> Dict:
    mp_manager = mp.Manager()
    mp_lock = mp_manager.Lock()
    warehouse_indexer = mp_manager.Value('i', 0)
    available_warehouses = mp_manager.list()
    occupied_warehouses = mp_manager.list()

    saved_warehouse_kwargs = {
        'mp_lock': mp_lock,
        'warehouse_indexer': warehouse_indexer,
        'available_warehouses': available_warehouses,
        'occupied_warehouses': occupied_warehouses,
    }
    return saved_warehouse_kwargs


class Warehouse:
    def __init__(self,
                 mp_lock: mp.Lock,
                 warehouse_indexer: mp.Value,
                 available_warehouses: List[str],
                 occupied_warehouses: List[str],
                 output_dir: str,
                 prefix: str = 'warehouse_',
                 suffix: str = '',
                 max_size: int = 12,
                 compress: bool = True):
        self.output_dir = output_dir
        self.prefix = prefix
        self.suffix = suffix
        self.max_size = max_size
        self.compress = compress

        self.mp_lock = mp_lock
        self.warehouse_indexer = warehouse_indexer
        self.available_warehouses = available_warehouses
        self.occupied_warehouses = occupied_warehouses

    def create_warehouse(self):
        # Fill 5 digits with 0s.
        curr_index = str(self.warehouse_indexer.value).zfill(5)
        new_filename_basename = f'{self.prefix}{curr_index}{self.suffix}'

        try:
            new_filename, new_metadata_filename = get_warehouse_filenames(new_filename_basename)
            new_filepath = os.path.join(self.output_dir, new_filename)
            new_metadata_filepath = os.path.join(self.output_dir, new_metadata_filename)
            with open(new_filepath, 'w') as f:
                f.truncate(0)
            with open(new_metadata_filepath, 'w') as f:
                f.truncate(0)
            self.available_warehouses.append(new_filename_basename)
            logging.debug(f'New warehouse created: {new_filename_basename}.')

        except:
            logging.error(f'Failed to create new warehouse: {new_filename_basename}.')

        self.warehouse_indexer.value += 1

    def assign_warehouse(self) -> str:
        """
        This function is intended to be called in main process (no parallelism).
        """
        with self.mp_lock:
            free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
            if len(free_warehouses) == 0:
                self.create_warehouse()
                free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]

            # Find the warehouse with the smallest index. Assign the warehouse.
            assigned_warehouse = min(free_warehouses)
            self.occupied_warehouses.append(assigned_warehouse)
            logging.debug(f'Assigning warehouse: {assigned_warehouse}.')

        return assigned_warehouse

    def bulk_assign(self, files: List[str]) -> Dict[str, List[str]]:
        """
        This function is intended to be called in main process (no parallelism).
        """
        free_warehouses: List[str] = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
        remaining_sizes = {
            w: self.max_size - get_file_size(os.path.join(self.output_dir, get_warehouse_filenames(w)[0]))
            for w in free_warehouses
        }
        acceptable_warehouses: List[Tuple[str, int]] = []
        warehouse_assignments = dict()

        def _sync_warehouses():
            nonlocal free_warehouses, remaining_sizes, acceptable_warehouses
            free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
            acceptable_warehouses = []
            for w in free_warehouses:
                if w not in remaining_sizes:
                    remaining_sizes[w] = self.max_size
                if remaining_sizes[w] > 0:
                    acceptable_warehouses.append((w, remaining_sizes[w]))
            acceptable_warehouses = sorted(acceptable_warehouses, key=lambda x: x[1], reverse=True)

        for file in files:
            _sync_warehouses()
            while not acceptable_warehouses:
                self.create_warehouse()
                _sync_warehouses()
                if not acceptable_warehouses:
                    logging.critical(f'Failed to create new warehouse for file: {file}.')
            min_size_warehouse, _ = acceptable_warehouses[0]
            if min_size_warehouse not in warehouse_assignments:
                warehouse_assignments[min_size_warehouse] = []
            warehouse_assignments[min_size_warehouse].append(file)
            remaining_sizes[min_size_warehouse] -= get_file_size(file)

        for warehouse in warehouse_assignments.keys():
            logging.debug(f'Bulk assigning warehouse: {warehouse}.')
            self.occupied_warehouses.append(warehouse)

        return warehouse_assignments

    def release_warehouse(self, warehouse: str) -> Union[str, None]:
        warehouse_file_should_compress = None

        with self.mp_lock:
            try:
                self.occupied_warehouses.remove(warehouse)

                logging.debug(f'Releasing warehouse: {warehouse}.')

                # Check current size, if it is larger than the max size, remove it from the available warehouses.
                warehouse_file, warehouse_metadata_file = get_warehouse_filenames(warehouse)
                warehouse_file_size = get_file_size(os.path.join(self.output_dir, warehouse_file))
                if warehouse_file_size >= self.max_size:
                    self.available_warehouses.remove(warehouse)
                    if self.compress:
                        warehouse_file_should_compress = warehouse_file

            except:
                logging.error(f'Warehouse [{warehouse}] does not exist.')

        return warehouse_file_should_compress

    def finalize_warehouse(self, warehouse: str):
        with self.mp_lock:
            self.available_warehouses.remove(warehouse)


def get_warehouse_filenames(basename: str) -> Tuple[str, str]:
    filename = f'{basename}.jsonl'
    metadata_filename = f'{basename}.metadata'
    return filename, metadata_filename


def get_file_size(filepath: str) -> float:
    if not os.path.exists(filepath):
        return 0
    return os.path.getsize(filepath) / (1000 ** 3)
