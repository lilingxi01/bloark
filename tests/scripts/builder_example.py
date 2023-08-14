import logging
import bloark


if __name__ == '__main__':
    # Create a builder instance with 8 processes and INFO-level logging.
    builder = bloark.Builder(output_dir='./tests/output', num_proc=1, log_level=logging.INFO)

    # Preload all files from the input directory (original data sources).
    # This command should be instant because it only loads paths rather than files themselves.
    builder.preload('./tests/sample_data/sample.bz2')

    # Start building the warehouses (this command will take a long time).
    builder.build()
