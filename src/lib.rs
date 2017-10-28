extern crate num_cpus;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar, LockResult, mpsc};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering::SeqCst;
use std::sync::mpsc::channel;
use std::thread;

/// A distributed cache of an iterator's items, meant to be filled by a background thread so that
/// worker threads can pull from a per-thread cache.
struct Hopper<T> {
    /// The cache of items, broken down into per-thread sub-caches.
    cache: Vec<Mutex<VecDeque<T>>>,
    /// A set of associated `Condvar`s for worker threads to wait on while the background thread
    /// adds more items to its cache.
    signals: Vec<Condvar>,
    /// A signal that tells whether or not the source iterator has been exhausted.
    ready: AtomicBool,
}

impl<T> Hopper<T> {
    /// Creates a new `Hopper` from the given iterator, with the given number of slots, and begins
    /// the background cache-filler worker.
    fn new<I>(iter: I, slots: usize) -> Arc<Hopper<T>>
    where
        I: Iterator<Item=T> + Send + 'static,
        T: Send + 'static,
    {
        let mut cache = Vec::with_capacity(slots);
        let mut signals = Vec::<Condvar>::with_capacity(slots);

        for _ in 0..slots {
            cache.push(Mutex::new(VecDeque::new()));
            signals.push(Condvar::new());
        }

        let ret = Arc::new(Hopper {
            cache,
            signals,
            ready: AtomicBool::new(false),
        });

        let hopper = ret.clone();

        thread::spawn(move || {
            let hopper = hopper;
            let mut current_slot = 0;

            for item in iter {
                let mut queue = guts(hopper.cache[current_slot].lock());
                queue.push_back(item);

                hopper.signals[current_slot].notify_one();

                current_slot = (current_slot + 1) % slots;
            }

            hopper.ready.store(true, SeqCst);

            for signal in &hopper.signals {
                signal.notify_all();
            }
        });

        ret
    }

    /// Loads an item from the given cache slot, potentially blocking while the cache-filler worker
    /// adds more items.
    fn get_item(&self, id: usize) -> Option<T> {
        let mut queue = guts(self.cache[id].lock());
        loop {
            //TODO: work-stealing
            if let Some(item) = queue.pop_front() {
                return Some(item);
            } else if self.ready.load(SeqCst) {
                return None;
            }

            queue = guts(self.signals[id].wait(queue));
        }
    }
}

/// A trait to extend `Iterator`s with consumers that work in parallel.
pub trait Polyester<T>
{
    /// Fold the iterator in parallel.
    ///
    /// This method works in two parts:
    ///
    /// 1. Use a set of threads to fold items individually into an accumulator,
    ///    whose initial state is a clone of `seed`.
    /// 2. Once each thread is finished with its own work set, collect each
    ///    intermediate accumulator into the final accumulator.
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

///Unwrap a LockResult to get the MutexGuard even when poisonsed.
///
///I don't anticipate ever poisoning my mutexes, but the alternative of just using unwrap gives me
///the creeps, so here we are.
///
///Source for the name: http://bulbapedia.bulbagarden.net/wiki/Guts_(Ability)
///
///Note that this is literally copy/pasted from synchronoise, where the name makes slightly more
///sense.
pub fn guts<T>(res: LockResult<T>) -> T {
    match res {
        Ok(guard) => guard,
        //The Pokemon's Guts raises its Attack!
        Err(poison) => poison.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::Polyester;

    #[test]
    fn basic_fold() {
        let par = (0..1_000_000).par_fold(0usize, |l,r| l+r, |l,r| l+r);
        let seq = (0..1_000_000).fold(0usize, |l,r| l+r);

        assert_eq!(par, seq);
    }

    #[test]
    fn basic_map() {
        let mut par = (0..100).par_map(|x| x*x).collect::<Vec<usize>>();
        let mut seq = (0..100).map(|x| x*x).collect::<Vec<usize>>();

        par.sort();
        seq.sort();

        assert_eq!(par, seq);
    }
}
