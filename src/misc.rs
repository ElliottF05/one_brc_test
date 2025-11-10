use std::{collections::{HashMap, HashSet}, hash::{DefaultHasher, Hash, Hasher}};

use regex::Regex;

use crate::CORRECT_RESULTS_PATH;

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