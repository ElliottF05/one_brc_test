use std::{collections::HashMap, fs::File, os::unix::fs::FileExt, thread, time::Instant};


use regex::Regex;

use crate::{CORRECT_RESULTS_PATH, MEASUREMENTS_PATH};

pub fn store_city_names() {
    let correct = std::fs::read_to_string(CORRECT_RESULTS_PATH).unwrap();
    let re = Regex::new(r"([^=]+)=([^,}]+)").unwrap();
    let correct_groups: Vec<_> = re.captures_iter(&correct)
        .map(|c| (c.get(1).unwrap().as_str(), c.get(2).unwrap().as_str()))
        .collect();

    let mut city_names = Vec::new();

    for (c_name, _) in correct_groups {
        let c_name = c_name.trim();
        let c_name = c_name.trim_start_matches(", ");
        let c_name = c_name.trim_start_matches("{");
        city_names.push(c_name);
    }

    city_names.sort_by_key(|n| n.len());
    println!("Shortest city names by byte length:");
    for i in 0..10 {
        println!("{} = {} bytes", city_names[i], city_names[i].len());
    }

    city_names.sort_by_key(|n| n.chars().count());
    println!("Shortest city names by number of utf8 characters:");
    for i in 0..10 {
        println!("{} = {} characters", city_names[i], city_names[i].chars().count());
    }

    let city_name_string = city_names.join("\n");
    std::fs::write("city_names.txt", city_name_string).unwrap();
}

fn get_u64_key(s: &str) -> (u64, String) {
    let bytes = s.as_bytes();
    let key = u64::from_le_bytes([
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[bytes.len()-3],
        bytes[bytes.len()-2],
        bytes[bytes.len()-1],
        bytes.len() as u8,
        0
    ]);

    let string = vec![
        (bytes[0] as char).to_string(),
        (bytes[1] as char).to_string(),
        (bytes[2] as char).to_string(),
        (bytes[bytes.len()-3] as char).to_string(),
        (bytes[bytes.len()-2] as char).to_string(),
        (bytes[bytes.len()-1] as char).to_string(),
        bytes.len().to_string()
    ].join("");

    return (key, string);
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

fn hash_3(name: &str, seed: u64) -> (String, u64) {
    let (key, string) = get_u64_key(name);

    let hash = mix64(key);
    let hash = hash * seed;
    let hash = hash % 32_768;

    return (string, hash);
}

pub fn find_seed() {
    let mut seed = 0;

    let binding = std::fs::read_to_string("city_names.txt").unwrap();
    let city_names: Vec<_> = binding.lines().collect();

    loop {
        let mut hashes = HashMap::new();

        let mut found_valid_seed = true;
        for name in &city_names {
            let (_, hash) = hash_3(name, seed);
            if hashes.contains_key(&hash) {
                found_valid_seed = false;
                break;
            } else {
                hashes.insert(hash, name);
            }
        }

        if found_valid_seed {
            println!("found valid seed: {}", seed);
            break;
        }

        seed += 1;
        if seed % 10_000 == 0 {
            println!("checking seed {}", seed);
        }
    }
}

pub fn test_hash_function() {
    let binding = std::fs::read_to_string("city_names.txt").unwrap();
    let city_names: Vec<_> = binding.lines().collect();

    let mut strings = HashMap::new();
    let mut hashes = HashMap::new();

    for name in city_names {
        let (string, hash) = hash_3(name, 384);
        if strings.contains_key(&string) {
            println!("Hash collision for cities {} and {} with string pattern {}", name, strings.get(&string).unwrap(), string);
        } else {
            strings.insert(string, name);
        }

        if hashes.contains_key(&hash) {
            println!("Hash collision for cities {} and {} with hash {}", name, hashes.get(&hash).unwrap(), hash);
        } else {
            hashes.insert(hash, name);
        }
    }
    println!("len strings: {}", strings.len());
    println!("len hashes: {}", hashes.len());
}

pub fn test_read_speed(num_threads: usize) {

    let start_time = Instant::now();

    fn read_chunk(file: File, start: usize, end: usize) -> usize {
        const BUF_SIZE: usize = 4 * 1024 * 1024;
        let mut buf = vec![0u8 ; BUF_SIZE].into_boxed_slice();

        let mut offset = start;
        let mut total_bytes_read = 0;

        while offset + BUF_SIZE <= end {
            let bytes_read = file.read_at(&mut buf, offset as u64).unwrap();
            offset += bytes_read;
            total_bytes_read += bytes_read;
        }

        return total_bytes_read;
    }

    let file = File::open(MEASUREMENTS_PATH).unwrap();
    let file_len = file.metadata().unwrap().len() as usize;

    let chunk_size = file_len / num_threads;
    
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let file_clone = file.try_clone().unwrap();
            thread::spawn( move || read_chunk(file_clone, i * chunk_size, (i+1) * chunk_size))
        })
        .collect();

    let total_bytes_read: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
    
    println!("TOTAL_BYTES_READ: {}", total_bytes_read);
    println!("TIME_ELAPSED: {}", start_time.elapsed().as_secs_f32())
}