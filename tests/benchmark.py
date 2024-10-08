import logging
import shutil
from tqdm import tqdm
import os
import time
import timeit

from .utils import get_mock_7z_temporary_dir
import bloark


def benchmark_runner():
    builder = bloark.Builder(output_dir='./tests/output', num_proc=os.cpu_count(), log_level=logging.ERROR)
    builder.preload(get_mock_7z_temporary_dir())
    builder.build()
    shutil.rmtree('./tests/output')


def main():
    repeat = 10
    durations = []
    for _ in tqdm(range(repeat), desc="Benchmarking"):
        duration = timeit.timeit(benchmark_runner, number=1)
        durations.append(duration)
        time.sleep(0.001)  # Adding a short delay to allow the progress bar to update.
    print('==============================')
    print(f'Average duration with {os.cpu_count()} CPUs: {round(sum(durations) / repeat, 6)} seconds.')

