# Definitions

We introduce some new concepts in this project, and it is important to understand them before you start using this package in order to avoid confusion and get a better experience.

## Block

Block is so called "single processable unit", which is the minimal structure that we should treat as a whole. For example, in Wikipedia Edit History scenario, a block is a single revision of an article because we cannot divide a revision into smaller pieces when the dataset stores "edit histories" rather than any other form of content.

## Warehouse

Warehouse is a file that stores a lot of blocks, and it is expected to be compressed. We call it *warehouse* because it is usually treated as a single-process file, which means that we should (but not must) only process blocks in one warehouse linearly. Plus, all related blocks should be stored in the same warehouse because the processing time between warehouses are not guaranteed to be linear.

```{warning}
Linear processing means that things are processed in an order, such as the line order within a file, or alphabetical order of files in a directory. For increasing resource efficiency, we do NOT process warehouses one-by-one in any order, thus processing tasks between warehouses should be independent by design.
```
