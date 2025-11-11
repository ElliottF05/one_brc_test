// Goal:
//      - Parallelization!!!!
//
// Change:
//      - Break up scan file into equal sized segments
//      - One thread per segment
//      - Use file.read_at to read at segment locations + offsets
//      - Use heap allocated buffers (buf and CustomHashMap.backing) to avoid stack overflow
//      
//
// Result:
//      - Time taken is now around 4s, around a great 72% improvement!!
//
// Analysis:
//      - Parallelism is cool


use std::{fs::File, i32, os::unix::fs::FileExt, simd::{Simd, cmp::SimdPartialEq, u8x16}, thread};

use memchr::memchr;

pub fn run(measurements_path: &str) -> String {
    const NUM_SEGMENTS: usize = 7;
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let split_indices = find_segment_splits(&measurements_file, NUM_SEGMENTS);

    let handles: Vec<_> = split_indices
        .into_iter()
        .map(|(start, end)| {
            let file = measurements_file.try_clone().unwrap();
            thread::spawn(move || {
                scan_file_segment(&file, start, end)
            })
        })
        .collect();
    
    let maps: Vec<_> = handles
        .into_iter()
        .map(|h| 
            h.join().unwrap()
        )
        .collect();
    
    let mut merged_map = CustomHashMap::new();
    for i in 0..merged_map.backing.len() {
        if maps[0].backing[i].count == 0 {
            continue;
        }
        let accum = &mut merged_map.backing[i];
        for j in 0..NUM_SEGMENTS {
            let other = &maps[j].backing[i];
            accum.merge_with(other);
        }
    }

    return format_output(&merged_map);
}

fn find_segment_splits(file: &File, num_segments: usize) -> Vec<(usize, usize)> {
    let file_len = file.metadata().unwrap().len() as usize;
    let expected_segment_size = file_len / num_segments;

    let buf: &mut [u8] = &mut [0u8 ; 64];

    let mut prev = 0;
    let mut split_indices = vec![];
    for i in 1..num_segments {
        let search_start = i * expected_segment_size;
        file.read_exact_at(buf, search_start as u64).unwrap();
        let j = buf.iter().position(|c| *c == b'\n').unwrap();

        let curr = search_start + j + 1;
        split_indices.push((prev, curr));
        prev = curr;
    }
    split_indices.push((prev, file_len));

    return split_indices;
}

fn scan_file_segment(file: &File, start_pos: usize, end_pos: usize) -> CustomHashMap {
    const BUF_SIZE: usize = 16 * 1024 * 1024;
    let mut buf = vec![0u8; BUF_SIZE];
    let mut offset = start_pos;

    let mut map = CustomHashMap::new();

    loop {
        // read the next chunk
        let bytes_read = file.read_at(&mut buf, offset as u64).unwrap();
        if bytes_read < BUF_SIZE {
            buf.truncate(bytes_read);
        }

        // main line reading loop
        let mut line_start = 0;
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

        // advance offset and break when we've read the entire file segment
        offset += line_start;
        if offset >= end_pos {
            break;
        }
    }
    return map;
}

#[inline(always)]
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

#[inline(always)]
fn first_match_in_u8x16(v: u8x16, target: u8) -> Option<usize> {
    let mask = v.simd_eq(Simd::splat(target));
    let bits = mask.to_bitmask();
    if bits == 0 {
        None
    } else {
        Some(bits.trailing_zeros() as usize)
    }
}

#[inline(always)]
fn parse_temp(line: &[u8]) -> i32 {
    let mut temp = 0;
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



#[derive(Debug, Clone)]
struct StationData {
    min_temp: i32,
    max_temp: i32,
    total: i32,
    count: u32,
    name: Option<Vec<u8>>,
}

impl StationData {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            min_temp: i32::MAX,
            max_temp: i32::MIN,
            total: 0,
            count: 0,
            name: None
        }
    }
    #[inline(always)]
    pub fn add_temp(&mut self, temp: i32, name: &[u8]) {
        self.min_temp = self.min_temp.min(temp);
        self.max_temp = self.max_temp.max(temp);
        self.total += temp;
        self.count += 1;
        if self.name.is_none() {
            self.name = Some(name.to_vec());
        }
    }
    #[inline(always)]
    pub fn merge_with(&mut self, other: &StationData) {
        self.min_temp = self.min_temp.min(other.min_temp);
        self.max_temp = self.max_temp.max(other.max_temp);
        self.total += other.total;
        self.count += other.count;
        if self.name.is_none() {
            self.name = other.name.clone();
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
    backing: Vec<StationData>,
}

impl CustomHashMap {
    pub fn new() -> Self {
        Self {
            backing: vec![StationData::new() ; 32_768]
        }
    }
    #[inline(always)]
    pub fn get_mut(&mut self, key: &[u8]) -> &mut StationData {
        let u64_key = get_u64_key(key);
        let hashed_key = mix64(u64_key);
        let index = hashed_key as usize & (32_768 - 1);
        return &mut self.backing[index];
    }
}

#[inline(always)]
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

#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58476d1ce4e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}