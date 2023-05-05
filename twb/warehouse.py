import logging
import os
from typing import Tuple, Union, List, Dict


class Warehouse:
    def __init__(self,
                 output_dir: str,
                 prefix: str = 'warehouse_',
                 suffix: str = '',
                 max_size: int = 8,
                 compress: bool = True):
        self.output_dir = output_dir
        self.prefix = prefix
        self.suffix = suffix
        self.max_size = max_size
        self.compress = compress

        self.warehouse_indexer = 0
        self.available_warehouses = []
        self.occupied_warehouses = []

    def create_warehouse(self):
        # Fill 5 digits with 0s.
        curr_index = str(self.warehouse_indexer).zfill(5)
        new_filename_basename = f'{self.prefix}{curr_index}{self.suffix}'

        try:
            new_filename, new_metadata_filename = get_warehouse_filenames(new_filename_basename)
            new_filepath = os.path.join(self.output_dir, new_filename)
            new_metadata_filepath = os.path.join(self.output_dir, new_metadata_filename)
            with open(new_filepath, 'w') as f:
                f.truncate(0)
            with open(new_metadata_filepath, 'w') as f:
                f.truncate(0)
            logging.debug(f'New warehouse created: {new_filename_basename}.')
            self.available_warehouses.append(new_filename_basename)
            self.warehouse_indexer += 1

        except:
            logging.error(f'Failed to create new warehouse: {new_filename_basename}.')

    def assign_warehouse(self) -> str:
        """
        This function is intended to be called in main process (no parallelism).
        """
        free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
        if len(free_warehouses) == 0:
            self.create_warehouse()
            free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]

        # Find the warehouse with the smallest size.
        min_size = float('inf')
        min_size_warehouse = None
        for warehouse in free_warehouses:
            warehouse_file, warehouse_metadata_file = get_warehouse_filenames(warehouse)
            warehouse_file_size = get_file_size(os.path.join(self.output_dir, warehouse_file))
            if warehouse_file_size < min_size:
                min_size = warehouse_file_size
                min_size_warehouse = warehouse

        # Assign the warehouse.
        self.occupied_warehouses.append(min_size_warehouse)

        logging.debug(f'Assigning warehouse: {min_size_warehouse}.')

        return min_size_warehouse

    def bulk_assign(self, files: List[str]) -> Dict[str, List[str]]:
        """
        This function is intended to be called in main process (no parallelism).
        """
        free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
        remaining_sizes = {
            w: self.max_size - get_file_size(os.path.join(self.output_dir, get_warehouse_filenames(w)[0]))
            for w in free_warehouses
        }
        warehouse_assignments = dict()

        def _sync_warehouses():
            nonlocal free_warehouses, remaining_sizes
            free_warehouses = [w for w in self.available_warehouses if w not in self.occupied_warehouses]
            for w in free_warehouses:
                if w not in remaining_sizes:
                    remaining_sizes[w] = self.max_size

        def _get_acceptable_warehouses(needed_size):
            nonlocal free_warehouses, remaining_sizes
            acceptable_warehouses = []
            for w in free_warehouses:
                if w not in remaining_sizes:
                    remaining_sizes[w] = self.max_size
                if remaining_sizes[w] >= needed_size:
                    acceptable_warehouses.append((w, remaining_sizes[w]))
            return acceptable_warehouses

        for file in files:
            file_size = get_file_size(file)
            _sync_warehouses()
            acceptable_warehouses = _get_acceptable_warehouses(needed_size=file_size)
            while not acceptable_warehouses:
                self.create_warehouse()
                _sync_warehouses()
                acceptable_warehouses = _get_acceptable_warehouses(needed_size=file_size)
                if not acceptable_warehouses:
                    logging.critical(f'Failed to create new warehouse for file: {file}.')
            min_size_warehouses = min(acceptable_warehouses, key=lambda x: x[1])
            min_size_warehouse = min_size_warehouses[0]
            if min_size_warehouse not in warehouse_assignments:
                warehouse_assignments[min_size_warehouse] = []
            warehouse_assignments[min_size_warehouse].append(file)
            remaining_sizes[min_size_warehouse] -= file_size

        for warehouse in warehouse_assignments.keys():
            logging.debug(f'Bulk assigning warehouse: {warehouse}.')
            self.occupied_warehouses.append(warehouse)

        return warehouse_assignments

    def release_warehouse(self, warehouse: str) -> Union[str, None]:
        try:
            self.occupied_warehouses.remove(warehouse)

            logging.debug(f'Releasing warehouse: {warehouse}.')

            # Check current size, if it is larger than the max size, remove it from the available warehouses.
            warehouse_file, warehouse_metadata_file = get_warehouse_filenames(warehouse)
            warehouse_file_size = get_file_size(os.path.join(self.output_dir, warehouse_file))
            if warehouse_file_size >= self.max_size:
                self.available_warehouses.remove(warehouse)
                if self.compress:
                    return warehouse_file

        except:
            logging.error(f'Warehouse [{warehouse}] does not exist.')

        return None

    def finalize_warehouse(self, warehouse: str):
        self.available_warehouses.remove(warehouse)


def get_warehouse_filenames(basename: str) -> Tuple[str, str]:
    filename = f'{basename}.jsonl'
    metadata_filename = f'{basename}.metadata'
    return filename, metadata_filename


def get_file_size(filepath: str) -> float:
    if not os.path.exists(filepath):
        return 0
    return os.path.getsize(filepath) / (1000 ** 3)
