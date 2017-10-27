extern crate num_cpus;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;

fn hopper<I, T>(iter: I, slots: usize) -> Arc<Vec<Mutex<VecDeque<T>>>>
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

/// A trait to extend `Iterator`s with consumers that work in parallel.
pub trait Polyester<T> {
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
                    let item = {
                        match hopper[id].lock() {
                            Ok(mut queue) => if let Some(item) = queue.pop_front() {
                                item
                            } else {
                                //TODO: work-stealing
                                break;
                            },
                            Err(_) => break,
                        }
                    };

                    acc = inner_fold(acc, item);
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
}
