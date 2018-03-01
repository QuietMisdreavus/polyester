// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

//! An extension trait to provide `Iterator` adapters that process items in parallel.
//!
//! The heart of this crate (and what should be considered its entry point) is the [`Polyester`]
//! trait, which is applied to any `Iterator` where it and its items are `Send`.
//!
//! [`Polyester`]: trait.Polyester.html

#![doc(test(attr(allow(unused_variables))))]

extern crate num_cpus;
extern crate crossbeam_deque;
extern crate synchronoise;
extern crate rayon;

pub mod par_iter;

use std::sync::{Arc, mpsc};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::channel;

use crossbeam_deque::{Deque, Stealer, Steal};
use synchronoise::{SignalEvent, SignalKind};

/// A trait to extend `Iterator`s with consumers that work in parallel.
///
/// This trait is applied to any iterator where it and its items are `Send`, allowing them to be
/// processed by multiple threads. Importing the trait into your code allows these adaptors to be
/// used like any other iterator adaptor - the only difference is that between the time they're
/// started and when they finish, they'll have spawned a number of threads to perform their work.
///
/// # Implementation Note
///
/// It's worth noting that even though this promises parallel processing, that's no guarantee that
/// it will be faster than just doing it sequentially. The iterator itself is a bottleneck for the
/// processing, since it needs an exclusive `&mut self` borrow to get each item. This library
/// attempts to get around that by draining the items in a separate thread into a cache that the
/// workers load from, but the synchronization costs for this mean that switching `map` for
/// `par_map` (for example) is not a universal win. Because of this, these adapters are only
/// expected to speed up processing if your source iterator is rather quick, and the closures you
/// hand to the adapters are not.
pub trait Polyester<T>
{
    /// Fold the iterator in parallel.
    ///
    /// This method works in two parts:
    ///
    /// 1. Use a set of threads to fold items individually into a per-thread "sub-accumulator"
    ///    using `inner_fold`. Each per-thread sub-accumulator begins with a clone of `seed`.
    /// 2. Once the source iterator is exhausted and has no more items, collect each intermediate
    ///    sub-accumulator into a final accumulator, starting with the first thread's personal
    ///    sub-accumulator and folding additional sub-accumulators using `outer_fold`.
    ///
    /// If there are no items in the iterator, `seed` is returned untouched.
    ///
    /// # Example
    ///
    /// ```
    /// use polyester::Polyester;
    /// # fn some_expensive_computation(it: usize) -> usize {
    /// #     if it == 7 { std::thread::sleep(std::time::Duration::from_secs(1)); }
    /// #     it
    /// # }
    ///
    /// let my_results = (0..1_000_000).par_fold(
    ///     vec![],
    ///     |mut acc, it| {
    ///         acc.push(some_expensive_computation(it));
    ///         acc
    ///     },
    ///     |mut left, right| {
    ///         left.extend(right);
    ///         left
    ///     }
    /// );
    /// ```
    fn par_fold<Acc, InnerFold, OuterFold>(
        self,
        seed: Acc,
        inner_fold: InnerFold,
        outer_fold: OuterFold,
    ) -> Acc
    where
        Acc: Clone + Send,
        InnerFold: Fn(Acc, T) -> Acc + Send + Sync,
        OuterFold: Fn(Acc, Acc) -> Acc;

    /// Maps the given closure onto each element in the iterator, in parallel.
    ///
    /// The `ParMap` adaptor returned by this function starts up a thread pool to run `map` on each
    /// item of the iterator. The result of each `map` is then passed back to the calling thread,
    /// where it can then be returned by `ParMap`'s `Iterator` implementation. Note that `ParMap`
    /// will yield items in the order the *threads* return them, which may not be the same as the
    /// order the *source iterator* does.
    ///
    /// The `ParMap` adaptor does not start its thread pool until it is first polled, after which
    /// it will block for the next item until the iterator is exhausted.
    ///
    /// # Example
    ///
    /// ```
    /// use polyester::Polyester;
    /// # fn some_expensive_computation(it: usize) -> usize {
    /// #     if it == 7 { std::thread::sleep(std::time::Duration::from_secs(1)); }
    /// #     it
    /// # }
    ///
    /// let my_results = (0..1_000_000).par_map(|it| some_expensive_computation(it))
    ///                                .collect::<Vec<_>>();
    /// ```
    fn par_map<Map, Out>(self, map: Map) -> ParMap<Self, Map, Out>
    where
        Self: Sized,
        T: Send + 'static,
        Map: Fn(T) -> Out + Send + Sync + 'static,
        Out: Send + 'static;
}

impl<T, I> Polyester<T> for I
where
    I: Iterator<Item=T> + Send,
    T: Send,
{
    fn par_fold<Acc, InnerFold, OuterFold>(
        self,
        seed: Acc,
        inner_fold: InnerFold,
        outer_fold: OuterFold,
    ) -> Acc
    where
        Acc: Clone + Send,
        InnerFold: Fn(Acc, T) -> Acc + Send + Sync,
        OuterFold: Fn(Acc, Acc) -> Acc
    {
        let res = rayon::scope(|scope| {
            let num_jobs = rayon::current_num_threads();

            if num_jobs == 1 {
                //it's not worth collecting the items into the hopper and spawning a thread if it's
                //still going to be serial, just fold it inline
                return Err(self.fold(seed, inner_fold));
            }

            let hopper = Hopper::new_scoped(self, num_jobs, scope);
            let inner_fold = Arc::new(inner_fold);
            let (report, recv) = channel::<Acc>();

            //launch the workers
            for id in 0..num_jobs {
                let hopper = hopper.clone();
                let acc = seed.clone();
                let inner_fold = inner_fold.clone();
                let report = report.clone();
                scope.spawn(move |_| {
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

            Ok((seed, recv))
        });

        let mut acc: Option<Acc> = None;
        match res {
            Ok((seed, recv)) => {
                //collect and fold the workers' results
                for res in recv.iter() {
                    if acc.is_none() {
                        acc = Some(res);
                    } else {
                        acc = acc.map(|acc| outer_fold(acc, res));
                    }
                }

                acc.unwrap_or(seed)
            }
            Err(acc) => acc,
        }
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

/// A parallel `map` adapter, which uses multiple threads to process items.
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
            let num_jobs = rayon::current_num_threads();

            let hopper = Hopper::new(iter, num_jobs);
            let map = Arc::new(map);
            let (report, recv) = channel();

            //launch the workers
            for id in 0..num_jobs {
                let hopper = hopper.clone();
                let report = report.clone();
                let map = map.clone();
                rayon::spawn(move || {
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
    cache: Vec<Stealer<T>>,
    /// A set of associated `Condvar`s for worker threads to wait on while the background thread
    /// adds more items to its cache.
    signals: Vec<SignalEvent>,
    /// A signal that tells whether or not the source iterator has been exhausted.
    done: AtomicBool,
}

impl<T> Hopper<T> {
    /// Creates a new `Hopper` from the given iterator, with the given number of slots, and begins
    /// the background cache-filler worker.
    fn new_scoped<'a, I>(iter: I, slots: usize, scope: &rayon::Scope<'a>) -> Arc<Hopper<T>>
    where
        I: Iterator<Item=T> + Send + 'a,
        T: Send + 'a,
    {
        //the fillers go into the filler thread, the cache and signals go into the final hopper
        let mut fillers = Vec::with_capacity(slots);
        let mut cache = Vec::with_capacity(slots);
        let mut signals = Vec::<SignalEvent>::with_capacity(slots);

        for _ in 0..slots {
            let deque = Deque::new();
            let stealer = deque.stealer();
            fillers.push(deque);
            cache.push(stealer);
            //start the SignalEvents as "ready" in case the filler thread gets a heard start on the
            //workers
            signals.push(SignalEvent::new(true, SignalKind::Auto));
        }

        let ret = Arc::new(Hopper {
            cache: cache,
            signals: signals,
            done: AtomicBool::new(false),
        });

        let hopper = ret.clone();

        scope.spawn(move |_| {
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
            let deque = Deque::new();
            let stealer = deque.stealer();
            fillers.push(deque);
            cache.push(stealer);
            //start the SignalEvents as "ready" in case the filler thread gets a heard start on the
            //workers
            signals.push(SignalEvent::new(true, SignalKind::Auto));
        }

        let ret = Arc::new(Hopper {
            cache: cache,
            signals: signals,
            done: AtomicBool::new(false),
        });

        let hopper = ret.clone();

        rayon::spawn(move || {
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

    /// Attempts to steal an item from a single queue.
    fn steal(&self, id: usize) -> Option<T> {
        loop {
            match self.cache[id].steal() {
                Steal::Empty => return None,
                Steal::Data(it) => return Some(it),
                Steal::Retry => (),
            }
        }
    }

    /// Loads an item from the given cache slot, potentially blocking while the cache-filler worker
    /// adds more items.
    fn get_item(&self, id: usize) -> Option<T> {
        loop {
            //go pull from our cache to see if we have anything
            if let Some(item) = self.steal(id) {
                return Some(item);
            }

            //...but before we sleep, go check the other caches to see if they still have anything
            let mut current_id = id;
            loop {
                current_id = (current_id + 1) % self.cache.len();
                if current_id == id { break; }

                if let Some(item) = self.steal(current_id) {
                    return Some(item);
                }
            }

            //as a final check, see whether the filler thread is finished
            if self.done.load(SeqCst) {
                return None;
            }

            //otherwise, wait for the cache-filler to get more items
            self.signals[id].wait();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Polyester;
    use par_iter::AsParallel;
    use rayon::iter::ParallelIterator;
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
        let iter = (0..1_000_000).as_parallel().reduce(|| 0usize, |l,r| l+r);
        let after_iter = Instant::now();

        let par_dur = secs_millis(after_par.duration_since(before));
        let seq_dur = secs_millis(after_seq.duration_since(after_par));
        let iter_dur = secs_millis(after_iter.duration_since(after_seq));
        println!("");
        println!("    parallel fold:      {}.{:03}s", par_dur.0, par_dur.1);
        println!("    reduce as parallel: {}.{:03}s", iter_dur.0, iter_dur.1);
        println!("    sequential fold:    {}.{:03}s", seq_dur.0, seq_dur.1);

        assert_eq!(par, seq);
        assert_eq!(iter, seq);
    }

    #[test]
    fn basic_map() {
        let before = Instant::now();
        let mut par = (0..1_000_000).par_map(|x| x*x).collect::<Vec<usize>>();
        let after_par = Instant::now();
        let mut seq = (0..1_000_000).map(|x| x*x).collect::<Vec<usize>>();
        let after_seq = Instant::now();
        let mut iter = (0..1_000_000).as_parallel().map(|x| x*x).collect::<Vec<usize>>();
        let after_iter = Instant::now();

        par.sort();
        seq.sort();
        iter.sort();

        let par_dur = secs_millis(after_par.duration_since(before));
        let seq_dur = secs_millis(after_seq.duration_since(after_par));
        let iter_dur = secs_millis(after_iter.duration_since(after_seq));
        println!("");
        println!("    parallel map:    {}.{:03}s", par_dur.0, par_dur.1);
        println!("    map as parallel: {}.{:03}s", iter_dur.0, iter_dur.1);
        println!("    sequential map:  {}.{:03}s", seq_dur.0, seq_dur.1);

        assert_eq!(par, seq);
        assert_eq!(iter, seq);
    }
}
