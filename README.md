# polyester

some parallel iterator adapters for rust

This work-in-progress library features an extenstion trait, `Polyester`, that extends `Iterator` and
provides some parallel operations that allow you to spread consumption of the iterator across
multiple threads.

## but wait, isn't that what `rayon` does?

Not quite. `rayon` is built on creating adaptors that fit its own `ParallelIterator` trait, which
can only be created from fixed-length collections. `polyester` wants to work from arbitrary
`Iterators` and provide arbitrary `Iterator`s back (or consume them and provide some appropriate
output). Basically, it's purely an extension trait for anything that already implements `Iterator`.
This means that there are distinct design goals for `polyester` that give it different performance
characteristics from `rayon`.

The internal design of this library changes pretty rapidly as i consider the ramifications of
various design decisions, but you're welcome to take a look and offer suggestions to improve
performance or suggest additional adaptors.
