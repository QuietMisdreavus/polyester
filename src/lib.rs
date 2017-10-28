extern crate num_cpus;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, mpsc};
use std::sync::mpsc::channel;

type Hopper<T> = Arc<Vec<Mutex<VecDeque<T>>>>;

fn hopper<I, T>(iter: I, slots: usize) -> Hopper<T>
where
    I: Iterator<Item=T>,
{
    let mut hopper = Vec::with_capacity(slots);

    for _ in 0..slots {
        hopper.push(VecDeque::new());
    }

    //load up the hopper with items for the workers
    //TODO: don't drain the iterator first?
    let mut current_slot = 0;
    for item in iter {
        hopper[current_slot].push_back(item);
        current_slot += (current_slot + 1) % slots;
    }

    //convert the hopper to use mutexes so the threads can drain the queues
    Arc::new(hopper.into_iter().map(|v| { Mutex::new(v) }).collect())
}

fn get_item<T>(hopper: &Hopper<T>, id: usize) -> Option<T> {
    match hopper[id].lock() {
        Ok(mut queue) => if let Some(item) = queue.pop_front() {
            Some(item)
        } else {
            //TODO: work-stealing
            None
        },
        //TODO: poison will lose an entire batch of items, should i do something different?
        Err(_) => None,
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
    I: Iterator<Item=T>
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

        let hopper = hopper(self, num_jobs);
        let inner_fold = Arc::new(inner_fold);
        let (report, recv) = channel();

        let num_jobs = if hopper.len() < num_jobs {
            hopper.len()
        } else {
            num_jobs
        };

        //launch the workers
        for id in 0..num_jobs {
            let hopper = hopper.clone();
            let acc = seed.clone();
            let inner_fold = inner_fold.clone();
            let report = report.clone();
            std::thread::spawn(move || {
                let mut acc = acc;

                loop {
                    let item = get_item(&hopper, id);

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
    Iter: Iterator,
    Iter::Item: Send + 'static,
    Map: Fn(Iter::Item) -> T + Send + Sync + 'static,
    T: Send + 'static,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let (Some(iter), Some(map)) = (self.iter.take(), self.map.take()) {
            let num_jobs = num_cpus::get();

            let hopper = hopper(iter, num_jobs);
            let map = Arc::new(map);
            let (report, recv) = channel();

            let num_jobs = if hopper.len() < num_jobs {
                hopper.len()
            } else {
                num_jobs
            };

            //launch the workers
            for id in 0..num_jobs {
                let hopper = hopper.clone();
                let report = report.clone();
                let map = map.clone();
                std::thread::spawn(move || {
                    loop {
                        let item = get_item(&hopper, id);

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
