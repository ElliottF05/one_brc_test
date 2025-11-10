// Goal:
//      - Reduce hashing overhead by using a custom hash function.
//
// Change:
//      - Use custom hash function! See MyHasher, get_u64_key() and mix64()
//
// Result:
//      - Time taken is now around 70s, around a 18% improvement.
//
// Analysis:
//      - Time is distributed as follows:
//          - split_measurement_string: 36%
//          - hashing: 14.4%
//          - BufReader.read_line(): 48%

use std::{collections::HashMap, hash::{BuildHasher, Hasher}, io::{BufRead, BufReader}};

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let mut buf_reader = BufReader::new(measurements_file);
    let mut map: HashMap<String, StationData, BuildMyHasher> = HashMap::with_capacity_and_hasher(12_289, BuildMyHasher {});

    let mut string_buf = String::with_capacity(256);

    while buf_reader.read_line(&mut string_buf).unwrap() > 0 {
        process_line(&string_buf, &mut map);
        string_buf.clear();
    } 

    return format_output(&map);
}

fn process_line(line: &str, map: &mut HashMap<String, StationData, BuildMyHasher>) {
    let (name, temp) = split_measurement_string(line);
    if !map.contains_key(name) {
        map.insert(name.to_owned(), StationData::new());
    }
    map.get_mut(name).unwrap().add_temp(temp);
}

fn split_measurement_string(line: &str) -> (&str, f32) {
    let mut name_and_temp = line.split(';');
    let name = name_and_temp.next().unwrap();
    let temp: f32 = name_and_temp.next().unwrap().trim_end().parse().unwrap();

    return (name, temp);
}

fn format_output(map: &HashMap<String, StationData, BuildMyHasher>) -> String {

    let mut parts = map
        .iter()
        .map(|(name, data)| data.format_data_point(name))
        .collect::<Vec<_>>();
    parts.sort();

    let result = "{".to_owned() + &parts.join(", ") + "}";

    return result;
}



#[derive(Debug)]
struct StationData {
    min_temp: f32,
    max_temp: f32,
    total: f32,
    count: u32,
}

impl StationData {
    pub fn new() -> Self {
        Self {
            min_temp: f32::MAX,
            max_temp: f32::MIN,
            total: 0.0,
            count: 0
        }
    }

    pub fn add_temp(&mut self, temp: f32) {
        self.min_temp = self.min_temp.min(temp);
        self.max_temp = self.max_temp.max(temp);
        self.total += temp;
        self.count += 1;
    }

    pub fn format_data_point(&self, station_name: &str) -> String {
        return format!("{}={:.1}/{:.1}/{:.1}", station_name, self.min_temp, self.total / self.count as f32, self.max_temp);
    }
}


#[derive(Default)]
struct MyHasher {
    hash_value: u64,
}

impl Hasher for MyHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.hash_value = get_u64_key(bytes);
    }
    fn write_u8(&mut self, i: u8) {}
    fn finish(&self) -> u64 {
        let res = mix64(self.hash_value);
        return res;
    }
}

fn get_u64_key(bytes: &[u8]) -> u64 {
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
    return key;
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

#[derive(Default)]
struct BuildMyHasher {}

impl BuildHasher for BuildMyHasher {
    type Hasher = MyHasher;
    fn build_hasher(&self) -> Self::Hasher {
        MyHasher::default()
    }
}