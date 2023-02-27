<img src="https://imagedelivery.net/Dr98IMl5gQ9tPkFM5JRcng/49178640-2f6d-4c23-e56f-a48eca531200/HD" alt="TWB" />

# Temporal Wikipedia Blocks (TWB)

Temporal Wikipedia Blocks (TWB) is a powerful Python package designed to process the extensive edit history of Wikipedia pages into easily manageable and memory-friendly blocks. The package is specifically developed to enable efficient parallelization and composition of these blocks to facilitate faster processing and analysis of large Wikipedia datasets. The original design of this package is to build other Wikipedia-oriented datasets on top of it.

The package works by dividing the Wikipedia edit history into temporal blocks, which are essentially subsets of the complete dataset that are based on time intervals. These blocks can then be easily processed and analyzed without the need to load the entire dataset into memory.

## Installation

The package is available on PyPI and can be installed using pip:

```bash
pip install twb-project
```

## Benefits

- **Efficient**: The package is designed to be memory-friendly and can be easily parallelized to process large datasets.
- **Fast**: The package is designed to be fast and can be easily optimized to process large datasets.
- **Flexible**: The package is designed to be flexible and can be easily extended to support other types of blocks.
- **Composable**: The package is designed to be composable and can be easily combined with other packages to build other datasets.

## Specification

- Default compression method: ZStandard.
