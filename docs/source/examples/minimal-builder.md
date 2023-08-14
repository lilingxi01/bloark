# Minimal Builder Example

This is the minimal working version of a BloArk Builder. This script is intended to build the initial warehouses out from the original data sources, such as Wikipedia edit histories.

## Python script

Putting this script in the same directory as the bash script is recommended. This script will be executed by the bash script. For example, we name this script as `blocks_0_builder.py`.

```python
import logging
import bloark


if __name__ == '__main__':
    # Create a builder instance with 8 processes and INFO-level logging.
    builder = bloark.Builder(output_dir='./output', num_proc=8, log_level=logging.INFO)
    
    # Preload all files from the input directory (original data sources).
    # This command should be instant because it only loads paths rather than files themselves.
    builder.preload('./input')
    
    # For testing purposes, we only build the first 10 files.
    # This way of modification is possible, but not recommended in production.
    builder.files = builder.files[:10]
    
    # Start building the warehouses (this command will take a long time).
    builder.build()
```

## Bash script

```{note}
Check [cluster requirements](../architecture/cluster-requirements.md) for more details about cluster environment setup.
```

This is an example bash script that will be used by `sbatch` to submit the job to a cluster. Put the following script in the same directory as the Python script. For example, we name this script as `blocks_0_builder.sh`.

```bash
#!/bin/bash

#SBATCH --job-name=blocks_0
#SBATCH --partition=longq
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=14-00:00
#SBATCH --mem-per-cpu=6000
#SBATCH --output=log_%j.out
#SBATCH --error=log_%j.error

python blocks_0_builder.py
```

After activating the correct conda environment (having correct terminal prefix like `(an_environment_with_bloark)` if you are using conda), you can simply submit the job by executing the following command in the same directory as the bash script:

```bash
sbatch blocks_0_builder.sh
```
