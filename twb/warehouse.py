import logging
import os
from typing import Tuple, Union
import multiprocessing as mp


class Warehouse:
    def __init__(self,
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

        mp_manager = mp.Manager()
        self.mp_lock = mp_manager.Lock()
        self.warehouse_indexer = mp_manager.Value('i', 0)
        self.available_warehouses = mp_manager.list()
        self.occupied_warehouses = mp_manager.list()

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

        return assigned_warehouse

    def release_warehouse(self, warehouse: str) -> Union[str, None]:
        warehouse_file_should_compress = None

        with self.mp_lock:
            try:
                self.occupied_warehouses.remove(warehouse)

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
