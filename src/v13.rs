// Goal:
//      - Eliminate double counting of line bytes (once for \n and once for ;), clean up
//      custom_scan_file while I'm at it if possible
//
// Change:
//      - Do what I said above
//
// Result:
//      - Time taken is now around 18s, only around 5% improvement.
//
// Analysis:
//      - Time distribution in profiler is no longer reliable.
//      - However it still seems majority of time is spent on memcrh


use std::{fs::File, i32, io::{BufRead, BufReader}};

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let buf_reader = BufReader::with_capacity(16 * 16 * 1024, measurements_file);
    let mut map = CustomHashMap::new();

    custom_scan_file(buf_reader, &mut map);

    return format_output(&map);
}

fn custom_scan_file(mut buf_reader: BufReader<File>, map: &mut CustomHashMap) {
    let mut carry = Vec::with_capacity(256);

    loop {
        let buf_len;
        {
            // println!("SCANNING CHUNK");
            // get a direct reference to the next chunk from the reader
            let buf = buf_reader.fill_buf().unwrap();
            buf_len = buf.len();
            // println!("buf_len: {}", buf.len());

            // if buf is empty, we've reached the end so break
            if buf.is_empty() {
                // still need to check carry if its not empty
                if !carry.is_empty() {
                    let semicolon_pos = memchr::memchr(b';', &carry).unwrap();
                    let name_slice = &carry[..semicolon_pos];
                    let temp_slice = &carry[semicolon_pos+1..];
                    let temp = parse_temp(temp_slice);
                    map.get_mut(name_slice).add_temp(temp, name_slice);
                }
                break;
            }

            let mut line_start = 0;
            let mut iter = memchr::memchr2_iter(b';', b'\n', buf);
            // let mut iter = buf
            //     .iter()
            //     .enumerate()
            //     .filter(|(_,c)| **c == b';' || **c == b'\n')
            //     .map(|(i,_)| i);

            // first deal with carry (if it exists)
            if !carry.is_empty() {

                let semicolon_pos;
                let i = iter.next().unwrap();

                if buf[i] == b';' {
                    let j = iter.next().unwrap();
                    let extra = carry.len();
                    carry.extend_from_slice(&buf[..j]);
                    semicolon_pos = i + extra;
                    line_start = j + 1;
                } else {
                    carry.extend_from_slice(&buf[..i]);
                    semicolon_pos = memchr::memchr(b';', &carry).unwrap();
                    line_start = i + 1;
                }

                let name_slice = &carry[..semicolon_pos];
                let temp_slice = &carry[semicolon_pos+1..];
                let temp = parse_temp(temp_slice);
                map.get_mut(name_slice).add_temp(temp, name_slice);

                carry.clear();
            }

            // main line reading loop
            while let Some(semicolon_pos) = iter.next() {
                if let Some(endline_pos) = iter.next() {
                    let name_slice = &buf[line_start..semicolon_pos];
                    let temp_slice = &buf[semicolon_pos+1..endline_pos];
                    let temp = parse_temp(temp_slice);
                    map.get_mut(name_slice).add_temp(temp, name_slice);
                    line_start = endline_pos + 1;
                }
            }

            // put the leftover in carry
            if line_start < buf.len() {
                carry.extend_from_slice(&buf[line_start..]);
            }
        }

        buf_reader.consume(buf_len);
    }
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