// Goal:
//      - Eliminate overhead from HashMap and its full internal hashing, by using a fully custom
//      implementation
//
// Change:
//      - Use CustomHashMap struct, which handles accessing and hashing directly.
//
// Result:
//      - Time taken is now around 19s, around 17% improvement.
//
// Analysis:
//      - Time is distributed as follows:
//          - split_measurement_string: 51%
//          - parse_temp: 15%
//          - fill_buf: 9.4%
//          - StationData::add_temp: 6.9%
//          - hashing: 5.9%
//          - custom file reading: 5%


use std::{fs::File, i32, io::{BufRead, BufReader}};

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let buf_reader = BufReader::with_capacity(16 * 1024, measurements_file);
    let mut map = CustomHashMap::new();

    custom_scan_file(buf_reader, &mut map);

    return format_output(&map);
}

fn custom_scan_file(mut buf_reader: BufReader<File>, map: &mut CustomHashMap) {
    let mut carry = Vec::with_capacity(256);

    loop {
        let buf_len;
        {
            // get a direct reference to the next chunk from the reader
            let buf = buf_reader.fill_buf().unwrap();
            buf_len = buf.len();
            // println!("buf_len: {}", buf.len());

            // if buf is empty, we've reached the end so break
            if buf.is_empty() {
                // still need to check carry if its not empty
                if !carry.is_empty() {
                    process_line_bytes(&carry, map);
                }
                break;
            }

            // iterate through the buf
            let mut line_start = 0;
            let mut search_start = 0;
            while search_start < buf.len() {

                // use memchr to find match efficiently
                let sub = &buf[search_start..(search_start+128).min(buf.len())];
                let i = match memchr::memchr(b'\n', sub) {
                    Some(i) => search_start + i,
                    None => break
                };

                // normal rust iter approach
                // let sub = &buf[search_start..(search_start+128).min(buf.len())];
                // let i = match sub.iter().position(|c| *c == b'\n') {
                //     Some(i) => search_start + i,
                //     None => break
                // };

                // if carry isn't empty, we must prepend it to the section
                // note this is a rare case
                if !carry.is_empty() {
                    carry.extend_from_slice(&buf[line_start..i]);
                    process_line_bytes(&carry, map);
                    carry.clear();
                } else {
                    process_line_bytes(&buf[line_start..i], map);
                }

                line_start = i+1;
                search_start = line_start + 7;
            }

            // put the leftover in carry
            if line_start < buf.len() {
                carry.extend_from_slice(&buf[line_start..]);
            }
        }

        buf_reader.consume(buf_len);
    }
}

fn process_line_bytes(bytes: &[u8], map: &mut CustomHashMap) {
    let (name, temp) = split_measurement_string(bytes);
    map.get_mut(name).add_temp(temp, name);
}

fn split_measurement_string(line: &[u8]) -> (&[u8], i32) {
    let split_index = memchr::memchr(b';', line).unwrap();
    // let split_index = line.iter().position(|c| *c == b';').unwrap();

    let name = &line[..split_index];
    let temp_slice = &line[split_index+1..];
    // let name = unsafe { line.get_unchecked(0..split_index) };
    // let temp_slice = unsafe { line.get_unchecked(split_index+1..) };

    let temp = parse_temp(temp_slice);
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

fn format_output(map: &CustomHashMap) -> String {

    let mut parts = map.backing
        .iter()
        .filter(|data| data.count > 0)
        .map(|data| data.format_data_point())
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
    name: Option<Vec<u8>>,
}

impl StationData {
    pub fn new() -> Self {
        Self {
            min_temp: i32::MAX,
            max_temp: i32::MIN,
            total: 0,
            count: 0,
            name: None
        }
    }
    pub fn add_temp(&mut self, temp: i32, name: &[u8]) {
        self.min_temp = self.min_temp.min(temp);
        self.max_temp = self.max_temp.max(temp);
        self.total += temp;
        self.count += 1;
        if self.name.is_none() {
            self.name = Some(name.to_vec());
        }
    }
    pub fn format_data_point(&self) -> String {
        return format!("{}={:.1}/{:.1}/{:.1}", 
            String::from_utf8(self.name.clone().unwrap()).unwrap(), 
            0.1 * self.min_temp as f32, 
            0.1 * self.total as f32 / self.count as f32, 
            0.1 * self.max_temp as f32
        );
    }
}

struct CustomHashMap {
    backing: [StationData ; 12_289]
}

impl CustomHashMap {
    pub fn new() -> Self {
        Self {
            backing: core::array::from_fn(|_| StationData::new())
        }
    }
    pub fn get_mut(&mut self, key: &[u8]) -> &mut StationData {
        let u64_key = get_u64_key(key);
        let hashed_key = mix64(u64_key).wrapping_mul(384); // 384 is a magic seed
        let index = hashed_key as usize % self.backing.len();
        return &mut self.backing[index];
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