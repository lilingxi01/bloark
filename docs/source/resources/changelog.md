# Changelog

## v2.3.1 (Feb 26, 2024)
* [Fix] Resolve an issue where preloading path might include unwanted files such as `.DS_Store` on macOS, `.env`, etc.
* [Fix] Resolve an issue that reader crashes when processing metadata files or unwanted non-warehouse files.

## v2.3.0 (Aug 20, 2023)
* [Add] Mark `bloark.Modifier.start()` function as stable (removed unstable decorator). Add notes to the deprecated `bloark.Modifier.build()` function that it will be removed from the next minor release (v2.4).

## v2.2.0 (Aug 14, 2023)
* [Fix] Builder module not closing pool after finishing the task.
* [Fix] Modifier module does not push forward for some glitches after the huge update.

## v2.1.3 (Aug 11, 2023)
* [Fix] Unstable warning throws error that causes the program to crash.

## v2.1.2 (Aug 10, 2023)
* [Fix] Optimize an opaque naming pattern on `bloark.Modifier` class. Now you should use `modifier.start()` to start the modification process rather than `modifier.build()`.

## v2.1.1 (Aug 7, 2023)
* [Fix] Support route pattern for Windows system.

## v2.1.0 (Aug 7, 2023)
* [Add] Added `.bz2` file support to builder module.

## v2.0.2 (Jul 16, 2023)
* [Fix] Some minor bugs that bother the compatibility of the Modifier module.

## v2.0.1 (Jul 16, 2023)
* [Major] Rebuild the entire `bloark.Modifier` module for better performance, stability, and flexibility.

## v1.1.2 (May 23, 2023)
* [Fix] Some potential empty directory issue.
