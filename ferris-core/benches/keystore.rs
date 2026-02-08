//! Benchmarks for KeyStore operations

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use ferris_core::{Entry, KeyStore, RedisValue};

fn bench_get_set(c: &mut Criterion) {
    let store = KeyStore::default();
    let db = store.database(0);

    // Pre-populate with some keys
    for i in 0..1000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Entry::new(RedisValue::String(Bytes::from(format!("value:{}", i))));
        db.set(key, value);
    }

    let mut group = c.benchmark_group("keystore");

    group.bench_function("get_existing", |b| {
        let key = Bytes::from("key:500");
        b.iter(|| black_box(db.get(&key)))
    });

    group.bench_function("get_nonexistent", |b| {
        let key = Bytes::from("nonexistent");
        b.iter(|| black_box(db.get(&key)))
    });

    group.bench_function("set_new", |b| {
        let mut counter = 10000u64;
        b.iter(|| {
            let key = Bytes::from(format!("newkey:{}", counter));
            let value = Entry::new(RedisValue::String(Bytes::from("value")));
            db.set(key, value);
            counter += 1;
        })
    });

    group.bench_function("set_overwrite", |b| {
        let key = Bytes::from("key:500");
        b.iter(|| {
            let value = Entry::new(RedisValue::String(Bytes::from("newvalue")));
            db.set(key.clone(), value);
        })
    });

    group.finish();
}

fn bench_concurrent_access(c: &mut Criterion) {
    use std::sync::Arc;
    use std::thread;

    let store = Arc::new(KeyStore::default());

    // Pre-populate
    let db = store.database(0);
    for i in 0..1000 {
        let key = Bytes::from(format!("key:{}", i));
        let value = Entry::new(RedisValue::String(Bytes::from(format!("value:{}", i))));
        db.set(key, value);
    }

    c.bench_function("concurrent_reads", |b| {
        b.iter(|| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let store = Arc::clone(&store);
                    thread::spawn(move || {
                        let db = store.database(0);
                        for i in 0..100 {
                            let key = Bytes::from(format!("key:{}", i % 1000));
                            black_box(db.get(&key));
                        }
                    })
                })
                .collect();

            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_get_set, bench_concurrent_access);
criterion_main!(benches);
