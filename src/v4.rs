// Goal:
//      - Reduce BufReader memory overhead. Currently, I use BufReader.lines() which it turns out
//      - allocates a whole new (heap-allocated) String for each one the 1 billion lines!!!
//
// Change:
//      - Use BufReader.read_line(buf) instead. This takes in an existing buf and uses it instead
//      of allocating a completely new one for each line.
//
// Result:
//      - Time taken is now around 86s, around a 21% improvement.
//
// Analysis:
//      - Time is now spent in 3 main areas:
//          - split_measurement_string: 31%
//          - hashing: 28%
//          - BufReader.read_line(): 40%

use std::{collections::HashMap, io::{BufRead, BufReader}};

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

pub fn run(measurements_path: &str) -> String {
    let measurements_file = std::fs::File::open(measurements_path).unwrap();

    let mut buf_reader = BufReader::new(measurements_file);
    let mut map = HashMap::new();

    let mut string_buf = String::with_capacity(256);

    while buf_reader.read_line(&mut string_buf).unwrap() > 0 {
        process_line(&string_buf, &mut map);
        string_buf.clear();
    } 

    return format_output(&map);
}

fn process_line(line: &str, map: &mut HashMap<String, StationData>) {
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

fn format_output(map: &HashMap<String, StationData>) -> String {

    let mut parts = map
        .iter()
        .map(|(name, data)| data.format_data_point(name))
        .collect::<Vec<_>>();
    parts.sort();

    let result = "{".to_owned() + &parts.join(", ") + "}";

    return result;
}