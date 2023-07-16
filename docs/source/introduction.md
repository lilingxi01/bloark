# How does it work?

BloArk is a complicated system that has some different mind models within its design. It is a good idea to understand the architecture of BloArk before using it.

## Process

This architecture typically works with any revision-based data. In Wikipedia Edit History scenario, the following steps are followed:

1. **Building:** The Wikipedia edit history is first divided into several blocks that obey the following rules:
   - Each block reflects one revision from an article.
   - All revisions of an article will be stored in the same warehouse.
   - Each block could be independently processed or analyzed.

2. **Modifying:** The blocks can then be modified for different purposes, with the following benefits:
   - Easy: Defining modifiers is as easy as defining a function that just tells how each block should be edited.
   - Parallelization: The blocks can be processed in parallel, which significantly reduces the processing time.
   - Memory-friendly: The blocks are, and should be, small enough to be loaded into memory, which makes the processing more efficient.
   - Composition: The blocks can be composed to form a larger block, which can then be processed or analyzed as a whole.

3. **Reusing:** The blocks can be reused for further modifications, which saves the time and effort of building the blocks from scratch.

4. **Sharing:** As long as the blocks are stored in the same format and be read by BloArk, they can easily be shared and reused by other users in other machines.

## Design considerations

When BloArk is designed, the following scenarios (including but not limited) are considered:
- The *single processable unit* of a dataset is NOT too large to be loaded into memory.
- A long-running device, such as Slurm job. It means that typical Jupyter Notebook is runnable but not suitable for this scenario in following reasons:
   - The Jupyter Notebook is not designed for long-running tasks. When you exit the browser (or close the browser tab), the Jupyter Notebook running session will be terminated.
   - Logs on Jupyter Notebook are not persistent and are not user-friendly. The scrolling experience along a long log is very bad on Jupyter Notebook (and Jupyter Lab, including all Jupyter-based software).
