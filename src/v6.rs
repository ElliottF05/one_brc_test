// Goal:
//      - Improve string processing times, avoid UTF-8 as much as possible?
//
// Change:
//      - Parse raw bytes and avoid f32 conversions. Instead store decimal xy.z as xyz (tenths).
//      - Look at parse_temp(), and notice that everything takes a &[u8] instead of &str or String
//
// Result:
//      - Time taken is now around 42s, an amazing 40% improvement!
//
// Analysis:
//      - Time is distributed as follows:
//          - split_measurement_string: 15%
//          - hashing: 28%
//          - BufReader.read_line(): 56%

use std::{collections::HashMap, hash::{BuildHasher, Hasher}, i32, io::{BufRead, BufReader, Read}};

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let mut buf_reader = BufReader::with_capacity(65536, measurements_file);
    let mut map: HashMap<Vec<u8>, StationData, BuildMyHasher> = HashMap::with_capacity_and_hasher(12_289, BuildMyHasher {});

    let mut buf = Vec::with_capacity(256);

    while buf_reader.read_until(b'\n', &mut buf).unwrap() > 0 {
        process_line(&buf, &mut map);
        buf.clear();
    } 
    return format_output(&map);
}

fn process_line(line: &[u8], map: &mut HashMap<Vec<u8>, StationData, BuildMyHasher>) {
    let (name, temp) = split_measurement_string(line);
    if !map.contains_key(name) {
        map.insert(name.to_owned(), StationData::new());
    }
    map.get_mut(name).unwrap().add_temp(temp);
}

fn split_measurement_string(line: &[u8]) -> (&[u8], i32) {
    let split_index = line.iter().position(|c| *c == b';').unwrap();
    let name = &line[..split_index];
    let temp = parse_temp(&line[split_index+1..]);
    return (name, temp);
}

fn parse_temp(line: &[u8]) -> i32 {
    let mut temp: i32 = 0;
    for c in line {
        if c.is_ascii_digit() {
            temp *= 10;
            temp += (c - b'0') as i32
        }
    }
    if line[0] == b'-' {
        temp *= -1;
    }
    return temp;
}

fn format_output(map: &HashMap<Vec<u8>, StationData, BuildMyHasher>) -> String {

    let mut parts = map
        .iter()
        .map(|(name, data)| data.format_data_point(&String::from_utf8(name.to_vec()).unwrap()))
        .collect::<Vec<_>>();
    parts.sort();

    let result = "{".to_owned() + &parts.join(", ") + "}";

    return result;
}



#[derive(Debug)]
struct StationData {
    min_temp: i32,
    max_temp: i32,
    total: i32,
    count: u32,
}

impl StationData {
    pub fn new() -> Self {
        Self {
            min_temp: i32::MAX,
            max_temp: i32::MIN,
            total: 0,
            count: 0
        }
    }

    pub fn add_temp(&mut self, temp: i32) {
        self.min_temp = self.min_temp.min(temp);
        self.max_temp = self.max_temp.max(temp);
        self.total += temp;
        self.count += 1;
    }

    pub fn format_data_point(&self, station_name: &str) -> String {
        return format!("{}={:.1}/{:.1}/{:.1}", station_name, 0.1 * self.min_temp as f32, 0.1 * self.total as f32 / self.count as f32, 0.1 * self.max_temp as f32);
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