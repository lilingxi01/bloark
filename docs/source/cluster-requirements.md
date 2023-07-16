# Cluster requirements

You can use `bloark` package wherever you want. However, if you want to use it in a cluster environment, following considerations should be taken into account.

## Python environment

You need to make sure either one of the following conditions is satisfied when trying to submit a job to a cluster:
- The default `pip` (comes with the user) already contains the necessary packages (specifically `bloark`).
- The `bloark` package is installed into a `conda` environment, and the `conda` environment is activated in the same terminal that you use to submit the job (executing this bash script). Check out the [conda tips](#conda-tips) section for more details.

## Conda tips

You don't have to write any `conda activate` scripts within the bash file because it will create a lot of confusions on the Slurm side. You can simply use an activated conda environment to submit the job and the job will automatically be executed in the exact same environment.

```{note}
It is fine if you are using `pip` that comes with conda to install the `bloark`. As long as you are installing to a conda environment, it will be taken care of within the Slurm job.
```