// Goal:
//      - Improve split_measurement speed using memchr or simd
//
// Change:
//      - Aggregate slices and values first to hopefully have better cache locality and vectorization
//
// Result:
//      - Time taken is now around 24s, this is a 30% regression :(
//
// Analysis:
//      - N/A


use std::{fs::File, i32, os::unix::fs::FileExt};

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let mut map = CustomHashMap::new();

    custom_scan_file(measurements_file, &mut map);

    return format_output(&map);
}

fn custom_scan_file(file: File, map: &mut CustomHashMap) {
    const BUF_CAPACITY: usize = 16 * 1024;
    const VEC_CAPACITY: usize = 16 * 1024;

    let mut buf: &mut [u8] = &mut [0u8 ; BUF_CAPACITY];
    let mut offset = 0;

    let mut char_indices = Vec::with_capacity(VEC_CAPACITY);
    let mut name_slices = Vec::with_capacity(VEC_CAPACITY);
    let mut temp_slices = Vec::with_capacity(VEC_CAPACITY);

    loop {
        let bytes_read = match file.read_at(buf, offset) {
            Ok(bytes_read) => bytes_read,
            Err(_) => break
        };
        if bytes_read == 0 {
            break;
        }
        if buf.len() != bytes_read {
            buf = &mut buf[0..bytes_read];
        }

        // iterate through the buf, read alternating portions of \n and ;
        char_indices.extend(
            memchr::memchr2_iter(b'\n', b';',&buf)
        );

        // want last index to be a newline
        if buf[*char_indices.last().unwrap()] != b'\n' {
            char_indices.pop().unwrap();
        }

        // [; \n ; \n ; \n ; \n]

        // get name slices
        name_slices.push((0, char_indices[0]));
        name_slices.extend(
            char_indices
                .windows(2)
                .skip(1)
                .step_by(2)
                .map(
                    |vals| (vals[0]+1, vals[1])
                )   
        );
        
        // get temp slices
        temp_slices.extend(
            char_indices
                .windows(2)
                .step_by(2)
                .map(
                    |vals| (vals[0]+1, vals[1])
                ) 
        );

        name_slices
            .iter()
            .zip(&temp_slices)
            .for_each(|((name_left, name_right), (temp_left, temp_right))| {
                let name_bytes = &buf[*name_left..*name_right];
                let temp_bytes = &buf[*temp_left..*temp_right];
                let temp = parse_temp(temp_bytes);
                map
                    .get_mut(name_bytes)
                    .add_temp(temp, name_bytes);
            });
        
        let last_newline_index = char_indices.last().unwrap();
        offset += *last_newline_index as u64 + 1;
        
        char_indices.clear();
        name_slices.clear();
        temp_slices.clear();
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