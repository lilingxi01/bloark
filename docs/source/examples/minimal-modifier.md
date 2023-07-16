# Minimal Modifier Example

This is the minimal working version of a BloArk Modifier. This script is intended to modify the warehouses built by the Builder. For example, you can use this script to modify the warehouses into a format that is more suitable for your further analysis.

## Python script

Putting this script in the same directory as the bash script is recommended. This script will be executed by the bash script. For example, we name this script as `blocks_1_modifier.py`.

```python
import logging
import bloark


# Define a modifier profile.
class PTFModifier(bloark.ModifierProfile):
    def block(self, data: dict, metadata: dict):
        # TODO: Modify the data and metadata here.
        #  If you want to skip this block, simply return `None, metadata`.
        #  If you want to skip the entire segment, simply return `None, None`.
        #  Check documentation of Modifier for more details.
        return data, metadata


# Create a modifier instance with 8 processes (CPUs) and INFO-level logging.
reader = bloark.Modifier(output_dir='./output', num_proc=8, log_level=logging.INFO)

# Preload all files from the input directory (original warehouses).
reader.preload('./input')

# Add the modifier profile to the modifier instance.
reader.add_profile(PTFModifier())

# Start modifying the warehouses (this command will take a long time).
reader.build()
```

## Bash script

```{note}
Check [cluster requirements](../architecture/cluster-requirements.md) for more details about cluster environment setup.
```

This is an example bash script that will be used by `sbatch` to submit the job to a cluster. Put the following script in the same directory as the Python script. For example, we name this script as `blocks_1_modifier.sh`.

```bash
#!/bin/bash

#SBATCH --job-name=blocks_1
#SBATCH --partition=longq
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=8
#SBATCH --time=14-00:00
#SBATCH --mem-per-cpu=6000
#SBATCH --output=log_%j.out
#SBATCH --error=log_%j.error

python blocks_1_modifier.py
```

After activating the correct conda environment (having correct terminal prefix like `(an_environment_with_bloark)` if you are using conda), you can simply submit the job by executing the following command in the same directory as the bash script:

```bash
sbatch blocks_1_modifier.sh
```