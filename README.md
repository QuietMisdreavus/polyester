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

## architecture overview

The internal design of this library changes pretty rapidly as i consider the ramifications of
various design decisions, but you're welcome to take a look and offer suggestions to improve
performance or suggest additional adaptors. For the moment, this is the basic idea, as of this
writing (2017-10-30):

The major problem with wanting to spread an iterator's values across threads is that the iterator
itself becomes a bottleneck. There is only one source of iteration state, and it requires a `&mut
self` borrow to get the next item. The way that `polyester` currently deals with this is placing the
iterator into a background thread so it can be loaded into per-thread queues, which the worker
threads pick up at the same time.

There's a major drawback to this approach, though: If the worker threads are not expected to be
performing a lot of per-item work, this will always be slower than just doing it sequentially.
Therefore, the current version of `polyester` is only recommended if you need to perform intensive
(or erratically intensive) work per item, or cannot afford to collect the iterator beforehand (to
just work sequentially, or to hand off to `rayon` instead).

Anyway, once this "hopper" is prepared, handles to it are given to a number of worker threads, so
that they can run user-supplied closures on the items. Each thread has its own queue to load items
from, and if its own queue is empty it will begin walking forward through other thread's queues to
attempt to load more items before waiting. Each queue has an associated `Condvar` which the
cache-loader worker will signal periodically while filling items or once the iterator has been
exhausted.

From here, each adaptor has its own processing:

### `par_fold`

`par_fold` performs a basic folding operation with its items: Start from a `seed` accumulator, and
iteratively add items to it until you're done. However, since there are multiple folds happening at
the same time, there needs to be an additional step where all the "intermediate" accumulators are
brought together. This is done by offering each worker thread a channel to hand their finished
accumulator back to the calling thread, which performs the "outer fold" to merge all the
accumulators together.

### `par_map`

`par_map` has a simpler goal with a slightly more complicated implementation: Since it wants to turn
the parallel iterator back into a sequential one, it performs the given map in each thread before
handing each item back to another channel, whose `Receiver` is used in the corresponding `ParMap`
`Iterator` implementation.

Note that due the nature of the hopper creation and processing, **order is not guaranteed** for the
items that come out of either adapter. (In fact, for `par_fold` each thread is basically getting
every Nth item, and the `outer_fold` will need to reconstruct the ordering if it wants it.)
