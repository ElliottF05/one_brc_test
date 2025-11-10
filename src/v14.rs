// Goal:
//      - Try using SIMD!!
//
// Change:
//      - Do what I said above
//
// Result:
//      - Time taken is now around 14.3s, around a 20% improvement!
//
// Analysis:
//      - SIMD is awesome


use std::{fs::File, i32, io::{BufRead, BufReader}, simd::{Simd, cmp::SimdPartialEq, u8x16}};

use memchr::memchr;

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

            // first deal with carry (if it exists)
            if !carry.is_empty() {
                let newline_pos = buf.iter().position(|c| *c == b'\n').unwrap();
                carry.extend_from_slice(&buf[..newline_pos]);
                let semicolon_pos = carry.iter().position(|c| *c == b';').unwrap();

                let name_slice = &carry[..semicolon_pos];
                let temp_slice = &carry[semicolon_pos+1..];
                let temp = parse_temp(temp_slice);
                map.get_mut(name_slice).add_temp(temp, name_slice);

                carry.clear();
                line_start = newline_pos + 1;
            }

            // main line reading loop
            loop {
                let slice = &buf[line_start..];
                if let Some(newline_pos) = find_char(slice, b'\n') {
                    let semicolon_pos = find_char(slice, b';').unwrap();

                    let name_slice = &slice[..semicolon_pos];
                    let temp_slice = &slice[semicolon_pos+1..newline_pos];
                    let temp = parse_temp(temp_slice);
                    map.get_mut(name_slice).add_temp(temp, name_slice);

                    line_start += newline_pos + 1;
                } else {
                    break;
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

fn find_char(buf: &[u8], target: u8) -> Option<usize> {
    if buf.len() >= 48 {
        let first = u8x16::from_slice(&buf[..16]);
        if let Some(idx) = first_match_in_u8x16(first, target) {
            return Some(idx);
        }
        let second = u8x16::from_slice(&buf[16..32]);
        if let Some(idx) = first_match_in_u8x16(second, target) {
            return Some(16 + idx);
        }
        let third = u8x16::from_slice(&buf[32..48]);
        if let Some(idx) = first_match_in_u8x16(third, target) {
            return Some(32 + idx);
        }
        None
    } else {
        return memchr(target, buf);
    }
}

fn load_u8x16_padded(bytes: &[u8]) -> u8x16 {
    let mut arr = [0u8 ; 16];
    let len = bytes.len().min(16);
    arr[..len].copy_from_slice(bytes);
    u8x16::from_array(arr)
}

fn first_match_in_u8x16(v: u8x16, target: u8) -> Option<usize> {
    let mask = v.simd_eq(Simd::splat(target));
    let bits = mask.to_bitmask();
    if bits == 0 {
        None
    } else {
        Some(bits.trailing_zeros() as usize)
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