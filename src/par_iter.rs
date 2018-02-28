// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! Connecting traits and types to bridge `Iterator` and `ParallelIterator`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, AtomicBool, Ordering};

use crossbeam_deque::{Deque, Stealer, Steal};
use rayon::prelude::*;
use rayon::iter::plumbing::*;
use rayon::{scope, current_num_threads};

/// Conversion trait to convert an `Iterator` to a `ParallelIterator`.
///
/// This needs to be distinct from `IntoParallelIterator` because that trait is already implemented
/// on a few `Iterator`s, like `std::ops::Range`.
pub trait AsParallel {
    /// What is the type of the output `ParallelIterator`?
    type Iter: ParallelIterator<Item = Self::Item>;

    /// What is the `Item` of the output `ParallelIterator`?
    type Item: Send;

    /// Convert this type to a `ParallelIterator`.
    fn as_parallel(self) -> Self::Iter;
}

impl<T: IntoIterator + Send> AsParallel for T
    where T::Item: Send
{
    type Iter = IterParallel<T>;
    type Item = T::Item;

    fn as_parallel(self) -> Self::Iter {
        IterParallel {
            iter: self,
        }
    }
}

pub struct IterParallel<Iter> {
    iter: Iter,
}

impl<Iter: IntoIterator + Send> ParallelIterator for IterParallel<Iter>
    where Iter::Item: Send
{
    type Item = Iter::Item;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
        where C: UnindexedConsumer<Self::Item>
    {
        scope(|s| {
            let done = Arc::new(AtomicBool::new(false));
            let deque = Deque::new();
            let stealer = deque.stealer();

            let signal = done.clone();
            s.spawn(move |_| {
                let mut iter = self.iter.into_iter();

                loop {
                    if !signal.load(Ordering::SeqCst) { break; }

                    match iter.next() {
                        Some(it) => deque.push(it),
                        _ => break,
                    }
                }

                // if we got here, either the iterator is empty or the consumer is full - if the
                // former, let's signal to the consumer to stop allowing splits
                signal.store(true, Ordering::SeqCst);
            });

            let result = bridge_unindexed(IterParallelProducer {
                split_count: Arc::new(AtomicUsize::new(current_num_threads())),
                done: done.clone(),
                items: stealer,
            }, consumer);

            // if we're here, either the iterator is empty or the consumer is full - if the latter,
            // let's signal back to the iterator to stop
            done.store(true, Ordering::SeqCst);

            result
        })
    }
}

struct IterParallelProducer<T> {
    split_count: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    items: Stealer<T>,
}

// manual clone because T doesn't need to be Clone, but the derive assumes it should be
impl<T> Clone for IterParallelProducer<T> {
    fn clone(&self) -> Self {
        IterParallelProducer {
            split_count: self.split_count.clone(),
            done: self.done.clone(),
            items: self.items.clone(),
        }
    }
}

impl<T: Send> UnindexedProducer for IterParallelProducer<T> {
    type Item = T;

    fn split(self) -> (Self, Option<Self>) {
        let mut count = self.split_count.load(Ordering::SeqCst);

        loop {
            let done = self.done.load(Ordering::SeqCst);
            match count.checked_sub(1) {
                Some(new_count) if !done => {
                    let last_count = self.split_count.compare_and_swap(count, new_count, Ordering::SeqCst);
                    if last_count == count {
                        return (self.clone(), Some(self));
                    } else {
                        count = last_count;
                    }
                }
                _ => {
                    return (self, None);
                }
            }
        }
    }

    fn fold_with<F>(self, mut folder: F) -> F
        where F: Folder<Self::Item>
    {
        loop {
            match self.items.steal() {
                Steal::Data(it) => folder = folder.consume(it),
                Steal::Empty => return folder,
                Steal::Retry => (),
            }
        }
    }
}
