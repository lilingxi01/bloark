# Minimal Builder Example

This is the minimal working version of a BloArk Builder. This script is intended to build the initial warehouses out from the original data sources, such as Wikipedia edit histories.

## Python script

Putting this script in the same directory as the bash script is recommended. This script will be executed by the bash script. For example, we name this script as `blocks_0_builder.py`.

```python
import logging
import bloark


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

This is an example bash script that will be used by `sbatch` to submit the job to a cluster. You need to make sure either one of the following conditions is satisfied:
- The default `pip` (comes with the user) already contains the necessary packages (specifically `bloark`).
- The `bloark` package is installed into a `conda` environment, and the `conda` environment is activated in the same terminal that you use to submit the job (executing this bash script). Check out the [conda tips](#conda-tips) section for more details.

Put the following script in the same directory as the Python script. For example, we name this script as `blocks_0_builder.sh`.

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

### Conda tips

You don't have to write any `conda activate` scripts within the bash file because it will create a lot of confusions on the Slurm side. You can simply use an activated conda environment to submit the job and the job will be executed in the exact same environment.

```{note}
It is fine if you are using `pip` to install the `bloark`. As long as you are installing to a conda environment, it will be taken care of within the Slurm job.
```
