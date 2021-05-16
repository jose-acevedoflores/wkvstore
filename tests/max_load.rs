use rand;
use rand::prelude::IteratorRandom;
use rand::Rng;
use std::ops::Add;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use wkvstore;
use wkvstore::KVStore;

const START_KEYS: u32 = 10_000_000;
const SAMPLES: u32 = 1000;

fn spawn_run(
    num: u16,
    store: &KVStore<Vec<u8>>,
    keys: Vec<String>,
) -> JoinHandle<Vec<(String, u128)>> {
    let c1 = store.get_client();
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut results = Vec::new();
        for _ in 0..SAMPLES {
            thread::sleep(Duration::from_millis(rng.gen_range(100..900)));
            let k = keys.iter().choose(&mut rng).unwrap();

            let start = Instant::now();
            let v = c1.retrieve(k);
            let dur = start.elapsed();
            let s = format!("key {} val {:?} in {} nanos", k, v, dur.as_nanos());
            if v.is_none() {
                println!("Handle{} {} ", num, s);
            }
            results.push((s, dur.as_nanos()));
        }
        results
    })
}

#[test]
fn max_load() {
    let store = wkvstore::KVStore::<Vec<u8>>::new();
    let client = store.get_client();

    let mut rng = rand::thread_rng();

    let mut keys = Vec::new();

    for x in 1..START_KEYS {
        let v: f64 = rng.gen();
        let v = (x as f64) * v;
        let k = format!("mykey:{}-{:.8}", x, v);

        let with_exp = if x % 100 == 0 {
            keys.push(k.clone());
            if x % 34 == 0 {
                Some(
                    Duration::from_secs(rng.gen_range(1..20))
                        .add(Duration::from_millis(rng.gen_range(100..900))),
                )
            } else {
                None
            }
        } else {
            None
        };

        if let Some(exp) = with_exp {
            println!("WITH EXPIRATION IN {}, key{}", exp.as_secs_f64(), k);
            client.insert_with_expiration(&k, k.as_bytes().to_vec(), exp);
        } else {
            client.insert(&k, k.as_bytes().to_vec());
        }
    }

    let handle1 = spawn_run(1, &store, keys.clone());
    let handle2 = spawn_run(2, &store, keys.clone());
    let handle3 = spawn_run(3, &store, keys.clone());

    let mut res1 = handle1.join().unwrap();
    let mut res2 = handle2.join().unwrap();
    let mut res3 = handle3.join().unwrap();

    let mut max = 0;
    let mut avg = 0;

    let n = (res1.len() + res2.len() + res3.len()) as u128;

    res1.drain(..).for_each(|(_, dur)| {
        if dur > max {
            max = dur;
        }
        avg += dur;
    });

    res2.drain(..).for_each(|(_, dur)| {
        if dur > max {
            max = dur;
        }
        avg += dur;
    });

    res3.drain(..).for_each(|(_, dur)| {
        if dur > max {
            max = dur;
        }
        avg += dur;
    });

    println!("Stats: max={}ns avg={}ns", max, avg / n);
}
