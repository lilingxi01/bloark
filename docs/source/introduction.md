# How does it work?

This architecture typically works with any revision-based data. In Wikipedia Edit History scenario, the following steps are followed:

1. **Building:** The Wikipedia edit history is first divided into several blocks that obey the following rules:
   - Each block reflects one revision from an article.
   - All revisions of an article will be stored in the same warehouse.
   - Each block could be independently processed or analyzed.

by dividing the Wikipedia edit history into temporal blocks, which are essentially subsets of the complete dataset that are based on time intervals. These blocks can then be easily processed and analyzed without the need to load the entire dataset into memory.
