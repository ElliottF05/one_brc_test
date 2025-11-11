// Goal:
//      - One reader thread, multiple consumer threads
//
// Change:
//      - Implemented one treader thread, multiple consumer threads
//      
// Result:
//      - Still takes almost exactly 4s, but the reader thread spends 98% of its time on pread.
//      - I think I am IO blocked now :)
//
// Analysis:
//      - 4s, reader spends 98% of time on pread


use std::{fs::File, i32, os::unix::fs::FileExt, simd::{Simd, cmp::SimdPartialEq, u8x16}, sync::{Arc, Condvar, Mutex, atomic::{AtomicBool, Ordering}}, thread, vec};

use memchr::memchr;


// thin wrapper around a buf that contains length data
struct Chunk {
    buf: Box<[u8]>,
    len: usize,
}

// manages a pool of buffers used by threads
struct Pool<T> {
    inner: Mutex<Vec<T>>,
    cv: Condvar,
    closed: AtomicBool
}

impl<T> Pool<T> {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Vec::new()),
            cv: Condvar::new(),
            closed: false.into(),
        }
    }
    pub fn take(&self) -> Option<T> {
        let mut guard = self.inner.lock().unwrap();
        loop {
            if let Some(taken) = guard.pop() {
                return Some(taken);
            }

            // if pool is empty and closed, terminate
            if self.closed.load(Ordering::Relaxed) {
                return None;
            }

            // wait on condvar for pool to fill up again
            guard = self.cv.wait(guard).unwrap();
        }
    }
    pub fn put(&self, returned: T) {
        let mut guard = self.inner.lock().unwrap();
        guard.push(returned);
        self.cv.notify_one();
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::Relaxed);
        self.cv.notify_all();
    }
}

fn reader_thread(file: File, empty_bufs: Arc<Pool<Box<[u8]>>>, full_chunks: Arc<Pool<Chunk>>) {
    let file_len = file.metadata().unwrap().len() as usize;
    let mut offset = 0;

    while offset < file_len {

        // get an empty buf to read to
        let mut buf = empty_bufs.take().unwrap();

        // read into this buf
        let bytes_read = file.read_at(&mut buf, offset as u64).unwrap();
        let slice = &buf[..bytes_read];

        // truncate to last newline character in this buf
        let last_newline_pos = slice.iter().rposition(|c| *c == b'\n').unwrap();
        offset += last_newline_pos + 1;

        // put this chunk to full_chunks pool for a worker thread to use
        let chunk = Chunk { buf: buf, len: last_newline_pos + 1 };
        full_chunks.put(chunk);
    }

    full_chunks.close();
}

fn worker_thread(empty_bufs: Arc<Pool<Box<[u8]>>>, full_chunks: Arc<Pool<Chunk>>) -> CustomHashMap {
    let mut map = CustomHashMap::new();

    loop {
        // get buf to process
        let chunk = match full_chunks.take() {
            Some(chunk) => chunk,
            None => break
        };

        // main line reading loop
        let buf_slice = &chunk.buf[..chunk.len];
        let mut offset = 0;
        while offset < buf_slice.len() {

            let line_slice = &buf_slice[offset..];
            let newline_pos = find_char(line_slice, b'\n').unwrap();
            let semicolon_pos = find_char(line_slice, b';').unwrap();

            let name_slice = &line_slice[..semicolon_pos];
            let temp_slice = &line_slice[semicolon_pos+1..newline_pos];
            let temp = parse_temp(temp_slice);
            map.get_mut(name_slice).add_temp(temp, name_slice);

            offset += newline_pos + 1;
        }

        // return the buf to the empty_buf pool for the reader thread to fill
        empty_bufs.put(chunk.buf);
    }

    return map;
}


pub fn run(measurements_path: &str) -> String {
    const NUM_WORKERS: usize = 4;
    const NUM_BUFS: usize = 8;
    const BUF_SIZE: usize = 16 * 1024 * 1024;

    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    // create buf pools and fill empty bufs
    let empty_bufs = Arc::new(Pool::new());
    let full_chunks = Arc::new(Pool::new());
    for _ in 0..NUM_BUFS {
        empty_bufs.put(vec![0u8 ; BUF_SIZE].into_boxed_slice());
    }

    let reader_empty_bufs = empty_bufs.clone();
    let reader_full_bufs = full_chunks.clone();
    let _reader = thread::spawn( || {
        reader_thread(measurements_file, reader_empty_bufs, reader_full_bufs)
    });

    let workers: Vec<_> = (0..NUM_WORKERS)
        .map(|_| { 
            let worker_empty_bufs = empty_bufs.clone();
            let worker_full_bufs = full_chunks.clone();
            thread::spawn( || 
                worker_thread(worker_empty_bufs, worker_full_bufs)
            )
        })
        .collect();

    let maps: Vec<_> = workers
        .into_iter()
        .map( |h| 
            h.join().unwrap()
        )
        .collect();
    
    let mut merged_map = CustomHashMap::new();
    for i in 0..merged_map.backing.len() {
        if maps[0].backing[i].count == 0 {
            continue;
        }
        let accum = &mut merged_map.backing[i];
        for j in 0..NUM_WORKERS {
            let other = &maps[j].backing[i];
            accum.merge_with(other);
        }
    }

    return format_output(&merged_map);
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