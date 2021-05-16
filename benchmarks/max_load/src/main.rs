use chrono::prelude::*;

use rand;
use rand::prelude::IteratorRandom;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::fmt::Formatter;
use std::fs::File;
use std::io::Write;
use std::ops::Add;
use std::thread;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};
use wkvstore;
use wkvstore::{KVClient, KVStore};

//PARAMS
const NUMBER_OF_KEYS: u32 = 6_000_000;
const SAMPLES: u32 = 1_000_000;
const NUM_THREADS: u8 = 3;
const MAX_SLEEP_MILLIS: u64 = 5;

pub struct Stat {
    tid: u8,
    timestamp: u128,
    key: String,
    val_stringified: String,
    retrieve_duration_nanos: u128,
}

impl std::fmt::Display for Stat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}, thread{}, key, {}, val, {:?}, get duration, {} nanos\n",
            self.timestamp, self.tid, self.key, self.val_stringified, self.retrieve_duration_nanos
        )
    }
}

fn random_mutation(
    sample: u32,
    client: &KVClient<Vec<u8>>,
    keys: &Vec<String>,
    rng: &mut ThreadRng,
) {
    if sample % 5 != 0 {
        return;
    }

    let insert_or_delete = rng.gen_ratio(1, 2);
    let k = keys.iter().choose(rng).unwrap();
    if insert_or_delete {
        let exp = Duration::from_secs(rng.gen_range(1..18))
            .add(Duration::from_millis(rng.gen_range(100..900)));
        client.insert_with_expiration(k, k.as_bytes().to_vec(), exp);
    } else {
        client.delete(k);
    }
}

fn spawn_run(num: u8, store: &KVStore<Vec<u8>>, keys: Vec<String>) -> JoinHandle<Vec<Stat>> {
    let c1 = store.get_client();
    thread::spawn(move || {
        let mut rng = rand::thread_rng();
        let mut results = Vec::new();
        for sample in 0..SAMPLES {
            // thread::sleep(Duration::from_millis(rng.gen_range(1..MAX_SLEEP_MILLIS)));
            let key = keys.iter().choose(&mut rng).unwrap();

            let start = Instant::now();
            let val = c1.retrieve(key);
            let dur = start.elapsed();
            random_mutation(sample, &c1, &keys, &mut rng);

            let stat = Stat {
                tid: num,
                key: key.to_string(),
                val_stringified: format!("{:?}", val),
                retrieve_duration_nanos: dur.as_nanos(),
                timestamp: 0,
            };
            results.push(stat);
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

fn collect_stats(results: Vec<Vec<Stat>>, store_starting_size: usize, store_end_size: usize) {
    let mut max = 0;
    let mut avg = 0;
    let mut n = 0;
    let file_name = get_file_name();
    let mut file = File::create(&file_name).unwrap();

    for mut result in results {
        n += result.len() as u128;
        result.drain(..).for_each(|stat| {
            if stat.retrieve_duration_nanos > max {
                max = stat.retrieve_duration_nanos;
            }
            avg += stat.retrieve_duration_nanos;
            file.write(stat.to_string().as_bytes()).unwrap();
        });
    }

    let done = format!(
        "Stats: max={}ns avg={}ns. Store sizes = start:{} and end{}",
        max,
        avg / n,
        store_starting_size,
        store_end_size
    );
    log(&format!("{}. Saved to file at {}", done, file_name));
    file.write(done.as_bytes()).unwrap();
}

fn max_load() {
    let store = wkvstore::KVStore::<Vec<u8>>::new();
    let client = store.get_client();
    log("Perform setup");
    let keys = setup(&client);
    let store_starting_size = client.size();
    log(&format!("Setup complete {} ", store_starting_size));

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
    collect_stats(results, store_starting_size, client.size());
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
