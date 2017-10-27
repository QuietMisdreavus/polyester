extern crate num_cpus;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::sync::mpsc::channel;

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
        let mut hopper = HashMap::new();
        let num_jobs = num_cpus::get();

        //load up the hopper with items for the workers
        //TODO: don't drain the iterator first?
        let mut current_slot = 0;
        for item in self {
            hopper.entry(current_slot)
                  .or_insert_with(VecDeque::new)
                  .push_back(item);
            current_slot += (current_slot + 1) % num_jobs;
        }

        //convert the hopper to use mutexes so the threads can drain the queues
        let hopper: Arc<HashMap<usize, Mutex<VecDeque<T>>>> =
            Arc::new(hopper.into_iter().map(|(k, v)| {
                (k, Mutex::new(v))
            }).collect::<HashMap<_,_>>());
        let inner_fold = Arc::new(inner_fold);
        let mut receivers = vec![];

        //launch the workers
        for id in 0..num_jobs {
            let hopper = hopper.clone();
            let acc = seed.clone();
            let inner_fold = inner_fold.clone();
            let (report, recv) = channel();
            receivers.push(recv);
            std::thread::spawn(move || {
                let mut acc = acc;

                loop {
                    let item = {
                        match hopper[&id].lock() {
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

        //collect and fold the workers' results
        let mut acc: Option<Acc> = None;
        for recv in receivers {
            if let Ok(res) = recv.recv() {
                if acc.is_none() {
                    acc = Some(res);
                } else {
                    acc = acc.map(|acc| outer_fold(acc, res));
                }
            }
        }

        acc.unwrap_or(seed)
    }
}
