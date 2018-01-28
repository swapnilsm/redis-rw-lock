Inspired by Mateusz Kobos's solution - http://code.activestate.com/recipes/577803-reader-writer-lock-with-priority-for-writers/

The library implements a reader-writer lock to use in the second readers-writers problem with distributed python environment. In this problem, many readers can simultaneously access a share, and a writer has an exclusive access to this share.
Additionally, the following constraints should be met: 

- No reader should be kept waiting if the share is currently opened for reading unless a writer is also waiting for the share
- No writer should be kept waiting for the share longer than absolutely necessary.
