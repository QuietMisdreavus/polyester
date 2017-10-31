// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! An extension trait to provide `Iterator` adapters that process items in parallel.
//!
//! The heart of this crate is the [`Polyester`] trait, which is applied to any `Iterator` where it
//! and its items are `Send`.
//!
//! [`Polyester`]: trait.Polyester.html

extern crate num_cpus;
extern crate coco;
extern crate synchronoise;

use std::sync::{Arc, mpsc};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::channel;
use std::thread;

use coco::deque;
use synchronoise::{SignalEvent, SignalKind};

/// A trait to extend `Iterator`s with consumers that work in parallel.
pub trait Polyester<T>
{
    /// Fold the iterator in parallel.
    ///
    /// This method works in two parts:
    ///
    /// 1. Use a set of threads to fold items individually into a per-thread accumulator using
    ///    `inner_fold`. Each per-thread accumulator begins with a clone of `seed`.
    /// 2. Once each thread is finished with its own work set, collect each intermediate
    ///    accumulator into a final accumulator, starting with the first thread's personal
    ///    accumulator and folding additional sub-accumulators using `outer_fold`.
    ///
    /// If there are no items in the iterator, `seed` is returned untouched.
    fn par_fold<Acc, InnerFold, OuterFold>(
        self,
        seed: Acc,
        inner_fold: InnerFold,
        outer_fold: OuterFold,
    ) -> Acc
    where
        T: Send + 'static,
        Acc: Clone + Send + 'static,
        InnerFold: Fn(Acc, T) -> Acc + Send + Sync + 'static,
        OuterFold: Fn(Acc, Acc) -> Acc;

    /// Maps the given closure onto each element in the iterator, in parallel.
    ///
    /// This method will dispatch the items of this iterator into a thread pool. Each thread will
    /// take items from the iterator and run the given closure, yielding them back out as they are
    /// processed. Note that this means that the ordering of items of the result is not guaranteed
    /// to be the same as the order of items from the source iterator.
    ///
    /// The `ParMap` adaptor does not start its thread pool until it is first polled, after which
    /// it will block for the next item until the iterator is exhausted.
    fn par_map<Map, Out>(self, Map) -> ParMap<Self, Map, Out>
    where
        Self: Sized,
        T: Send + 'static,
        Map: Fn(T) -> Out + Send + Sync + 'static,
        Out: Send + 'static;
}

impl<T, I> Polyester<T> for I
where
    I: Iterator<Item=T> + Send + 'static,
{
    fn par_fold<Acc, InnerFold, OuterFold>(
        self,
        seed: Acc,
        inner_fold: InnerFold,
        outer_fold: OuterFold,
    ) -> Acc
    where
        T: Send + 'static,
        Acc: Clone + Send + 'static,
        InnerFold: Fn(Acc, T) -> Acc + Send + Sync + 'static,
        OuterFold: Fn(Acc, Acc) -> Acc
    {
        let num_jobs = num_cpus::get();

        if num_jobs == 1 {
            //it's not worth collecting the items into the hopper and spawning a thread if it's
            //still going to be serial, just fold it inline
            return self.fold(seed, inner_fold);
        }

        let hopper = Hopper::new(self, num_jobs);
        let inner_fold = Arc::new(inner_fold);
        let (report, recv) = channel();

        //launch the workers
        for id in 0..num_jobs {
            let hopper = hopper.clone();
            let acc = seed.clone();
            let inner_fold = inner_fold.clone();
            let report = report.clone();
            std::thread::spawn(move || {
                let mut acc = acc;

                loop {
                    let item = hopper.get_item(id);

                    if let Some(item) = item {
                        acc = inner_fold(acc, item);
                    } else {
                        break;
                    }
                }

                report.send(acc).unwrap();
            });
        }

        //hang up our initial channel so we don't wait for a response from it
        drop(report);

        //collect and fold the workers' results
        let mut acc: Option<Acc> = None;
        for res in recv.iter() {
            if acc.is_none() {
                acc = Some(res);
            } else {
                acc = acc.map(|acc| outer_fold(acc, res));
            }
        }

        acc.unwrap_or(seed)
    }

    fn par_map<Map, Out>(self, map: Map) -> ParMap<I, Map, Out>
    where
        Self: Sized,
        T: Send + 'static,
        Map: Fn(T) -> Out + Send + Sync + 'static,
        Out: Send + 'static
    {
        ParMap {
            iter: Some(self),
            map: Some(map),
            recv: None,
        }
    }
}

/// A parallel `map` adapter, which processes items in parallel.
///
/// This struct is returned by [`Polyester::par_map`]. See that function's documentation for more
/// details.
///
/// [`Polyester::par_map`]: trait.Polyester.html#method.par_map
pub struct ParMap<Iter, Map, T>
{
    iter: Option<Iter>,
    map: Option<Map>,
    recv: Option<mpsc::IntoIter<T>>,
}

impl<Iter, Map, T> Iterator for ParMap<Iter, Map, T>
where
    Iter: Iterator + Send + 'static,
    Iter::Item: Send + 'static,
    Map: Fn(Iter::Item) -> T + Send + Sync + 'static,
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some(iter), Some(map)) = (self.iter.take(), self.map.take()) {
            let num_jobs = num_cpus::get();

            let hopper = Hopper::new(iter, num_jobs);
            let map = Arc::new(map);
            let (report, recv) = channel();

            //launch the workers
            for id in 0..num_jobs {
                let hopper = hopper.clone();
                let report = report.clone();
                let map = map.clone();
                std::thread::spawn(move || {
                    loop {
                        let item = hopper.get_item(id);

                        if let Some(item) = item {
                            report.send(map(item)).unwrap();
                        } else {
                            break;
                        }
                    }
                });
            }

            self.recv = Some(recv.into_iter());
        }

        self.recv.as_mut().and_then(|r| r.next())
    }
}

/// A distributed cache of an iterator's items, meant to be filled by a background thread so that
/// worker threads can pull from a per-thread cache.
struct Hopper<T> {
    /// The cache of items, broken down into per-thread sub-caches.
    cache: Vec<deque::Stealer<T>>,
    /// A set of associated `Condvar`s for worker threads to wait on while the background thread
    /// adds more items to its cache.
    signals: Vec<SignalEvent>,
    /// A signal that tells whether or not the source iterator has been exhausted.
    done: AtomicBool,
}

impl<T> Hopper<T> {
    /// Creates a new `Hopper` from the given iterator, with the given number of slots, and begins
    /// the background cache-filler worker.
    fn new<I>(iter: I, slots: usize) -> Arc<Hopper<T>>
    where
        I: Iterator<Item=T> + Send + 'static,
        T: Send + 'static,
    {
        //the fillers go into the filler thread, the cache and signals go into the final hopper
        let mut fillers = Vec::with_capacity(slots);
        let mut cache = Vec::with_capacity(slots);
        let mut signals = Vec::<SignalEvent>::with_capacity(slots);

        for _ in 0..slots {
            let (filler, stealer) = deque::new();
            fillers.push(filler);
            cache.push(stealer);
            //start the SignalEvents as "ready" in case the filler thread gets a heard start on the
            //workers
            signals.push(SignalEvent::new(true, SignalKind::Manual));
        }

        let ret = Arc::new(Hopper {
            cache,
            signals,
            done: AtomicBool::new(false),
        });

        let hopper = ret.clone();

        thread::spawn(move || {
            let hopper = hopper;
            let fillers = fillers;
            let mut current_slot = 0;
            let mut rounds = 0usize;

            for item in iter {
                fillers[current_slot].push(item);

                current_slot = (current_slot + 1) % slots;
                if current_slot == 0 {
                    rounds += 1;
                }

                //every time we've added (slots * 2) items to each slot, wake up all the threads if
                //they're waiting on more items
                if (rounds % (slots * 2)) == 0 {
                    hopper.signals[current_slot].signal();
                }
            }

            //we're out of items, so tell all the workers that we're done
            hopper.done.store(true, SeqCst);

            //...and wake them up so they can react to the "done" signal
            for signal in &hopper.signals {
                signal.signal();
            }
        });

        ret
    }

    /// Loads an item from the given cache slot, potentially blocking while the cache-filler worker
    /// adds more items.
    fn get_item(&self, id: usize) -> Option<T> {
        loop {
            //go pull from our cache to see if we have anything
            if let Some(item) = self.cache[id].steal() {
                return Some(item);
            }

            //our cache is out of items, so toggle the signal off
            self.signals[id].reset();

            //...but before we sleep, go check the other caches to see if they still have anything
            let mut current_id = id;
            loop {
                current_id = (current_id + 1) % self.cache.len();
                if current_id == id { break; }

                if let Some(item) = self.cache[current_id].steal() {
                    return Some(item);
                }
            }

            //as a final check, see whether the filler thread is finished
            if self.done.load(SeqCst) {
                return None;
            }

            //finally, wait on more items
            self.signals[id].wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Polyester;
    use std::time::{Instant, Duration};

    fn secs_millis(dur: Duration) -> (u64, u32) {
        (dur.as_secs(), dur.subsec_nanos() / 1_000_000)
    }

    #[test]
    fn basic_fold() {
        let before = Instant::now();
        let par = (0..1_000_000).par_fold(0usize, |l,r| l+r, |l,r| l+r);
        let after_par = Instant::now();
        let seq = (0..1_000_000).fold(0usize, |l,r| l+r);
        let after_seq = Instant::now();

        let par_dur = secs_millis(after_par.duration_since(before));
        let seq_dur = secs_millis(after_seq.duration_since(after_par));
        println!("");
        println!("    parallel fold:   {}.{:04}s", par_dur.0, par_dur.1);
        println!("    sequential fold: {}.{:04}s", seq_dur.0, seq_dur.1);

        assert_eq!(par, seq);
    }

    #[test]
    fn basic_map() {
        let before = Instant::now();
        let mut par = (0..1_000_000).par_map(|x| x*x).collect::<Vec<usize>>();
        let after_par = Instant::now();
        let mut seq = (0..1_000_000).map(|x| x*x).collect::<Vec<usize>>();
        let after_seq = Instant::now();

        par.sort();
        seq.sort();

        let par_dur = secs_millis(after_par.duration_since(before));
        let seq_dur = secs_millis(after_seq.duration_since(after_par));
        println!("");
        println!("    parallel map:   {}.{:04}s", par_dur.0, par_dur.1);
        println!("    sequential map: {}.{:04}s", seq_dur.0, seq_dur.1);

        assert_eq!(par, seq);
    }
}
