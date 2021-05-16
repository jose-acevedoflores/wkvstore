use chrono::prelude::*;

use rand;
use rand::prelude::IteratorRandom;
use rand::Rng;
use std::fs::File;
use std::io::Write;
use std::ops::Add;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use wkvstore;
use wkvstore::{KVClient, KVStore};
use rand::rngs::ThreadRng;

//PARAMS
const NUMBER_OF_KEYS: u32 = 6_000_000;
const SAMPLES: u32 = 10_000;
const NUM_THREADS: u8 = 3;
const MAX_SLEEP_MILLIS: u64 = 5;


fn random_mutation (client: &KVClient<Vec<u8>>, keys: &Vec<String>, rng: &mut ThreadRng) {
    let insert_or_delete = rng.gen_ratio(1,2);
    let k = keys.iter().choose(rng).unwrap();
    if insert_or_delete {
        client.insert(k, k.as_bytes().to_vec());
    } else {
        client.delete(k);
    }
}

fn spawn_run(
    num: u8,
    store: &KVStore<Vec<u8>>,
    keys: Vec<String>,
) -> JoinHandle<Vec<(String, u128)>> {
    let c1 = store.get_client();
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut results = Vec::new();
        for sample in 0..SAMPLES {
            // thread::sleep(Duration::from_millis(rng.gen_range(1..MAX_SLEEP_MILLIS)));
            let k = keys.iter().choose(&mut rng).unwrap();

            let start = Instant::now();
            let v = c1.retrieve(k);
            let dur = start.elapsed();
            if sample % 5 ==0 {
                //every 5 samples perform a mutation (write or delete)
                random_mutation(&c1, &keys, &mut rng);
            }
            let s = format!(
                "thread{} key {} val {:?} in {} nanos\n",
                num,
                k,
                v,
                dur.as_nanos()
            );
            // if v.is_none() {
            // println!("Handle{} {} ", num, s);
            // }
            results.push((s, dur.as_nanos()));
        }
        results
    })
}

fn setup(client: &KVClient<Vec<u8>>) -> Vec<String> {
    let mut rng = rand::thread_rng();

    let mut keys_to_be_sampled = Vec::new();

    for key_num in 1..NUMBER_OF_KEYS {
        let rand_val: f64 = rng.gen();
        let rand_val = (key_num as f64) * rand_val;
        let key = format!("mykey:{}-{:.8}", key_num, rand_val);

        let should_have_expiration = if key_num % 10 == 0 {
            keys_to_be_sampled.push(key.clone());

            let keys_with_exp_threshold: f64 = rng.gen();
            if keys_with_exp_threshold > 0.5f64 {
                Some(
                    Duration::from_secs(rng.gen_range(60..70))
                        .add(Duration::from_millis(rng.gen_range(100..900))),
                )
            } else {
                None
            }
        } else {
            None
        };

        if let Some(expiration) = should_have_expiration {
            // println!("WITH EXPIRATION IN {}, key{}", exp.as_secs_f64(), k);
            client.insert_with_expiration(&key, key.as_bytes().to_vec(), expiration);
        } else {
            client.insert(&key, key.as_bytes().to_vec());
        }
    }

    keys_to_be_sampled
}

fn collect_stats(results: Vec<Vec<(String, u128)>>) {
    let mut max = 0;
    let mut avg = 0;
    let mut n = 0;
    let mut file = File::create(get_file_name()).unwrap();

    for mut result in results {
        n += result.len() as u128;
        result.drain(..).for_each(|(out, dur)| {
            if dur > max {
                max = dur;
            }
            avg += dur;
            file.write(out.as_bytes()).unwrap();
        });
    }

    let done = format!("Stats: max={}ns avg={}ns", max, avg / n);
    log(&done);
    file.write(done.as_bytes()).unwrap();
}

fn max_load() {
    let store = wkvstore::KVStore::<Vec<u8>>::new();
    let client = store.get_client();
    log("Perform setup");
    let keys = setup(&client);
    log(&format!("Setup complete {} ", client.size()));

    log(&format!("Spawning {} threads", NUM_THREADS));
    let handles: Vec<_> = (0..NUM_THREADS)
        .map(|idx| spawn_run(idx + 1, &store, keys.clone()))
        .collect();

    log(&format!("Spawned {} threads", NUM_THREADS));

    log("Waiting for results");
    let results: Vec<_> = handles
        .into_iter()
        .map(|result| result.join().unwrap())
        .collect();

    log("Results done, collecting stats");
    collect_stats(results);
    log(&format!("stats done, store size {}", client.size()));
}

fn log(msg: &str) {
    let utc: DateTime<Utc> = Utc::now();
    println!("[{}]  {}", utc, msg);
}

fn get_file_name() -> String {
    let utc: DateTime<Utc> = Utc::now();

    let res = format!(
        "{}-{}:{}:{}",
        utc.date(),
        utc.hour(),
        utc.minute(),
        utc.second()
    );
    format!(
        "/Users/joseacevedo/Desktop/wkvstore/benchmarks/max_load-{}.stats.txt",
        res
    )
}

fn main() {
    let start = Instant::now();
    log("Starting run");
    max_load();
    log(&format!(
        "Total runtime {} seconds",
        start.elapsed().as_secs_f64()
    ));
}

//RUNS
// Stats: max=884391ns avg=64145ns      test run: 623.20 seconds (1000 Samples) 100 to 900
// Stats: max=374247ns avg=67546ns      test run: 5140 (10000 Samples) 100 to 900
// Stats: max=266953ns avg=33161ns      test run: 1656.25 (10000 Samples) 100 to 200 sleeps
