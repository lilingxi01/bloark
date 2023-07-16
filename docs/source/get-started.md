# Get started

## Installation

### Via PyPI

The easiest way to install BloArk is via PyPI, you can simply run the following command in your terminal (or with any package manager you want, such as poetry):

```bash
pip install bloark
```

```{note}
If you are trying to use it in Slurm (cluster environment), it is recommended to install it in a virtual environment such as `virtualenv` or `conda`.
```

### Via distributable file from GitHub

Alternatively, you can download the distributable file from our GitHub repository and install it manually:

```bash
pip install path/to/bloark-0.0.0.tar.gz
```

```{warning}
This method is NOT recommended in Slurm (cluster environment) since you have to upload the file to the cluster first. There is no actual difference between this method and using PyPI.
```

## Suggested environment

BloArk is recommended to be run in cluster environment or simply in terminal. Jupyter Notebook session (including but not limited to Jupyter Lab) is not recommended since it is not designed for long-running tasks. You can check [design considerations](introduction.md#design-considerations) for more details.
