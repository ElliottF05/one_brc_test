// Goal:
//      - Reduce memory overhead and heap allocations from all the collect::<Vec>() calls.
//
// Change:
//      - Remove "collect()" call in line.split(":") in split_measurement_string(), instead
//      use iterators .next() method
//
// Result:
//      - Time taken is now around 150s, around a 25% improvement.
//
// Analysis:
//      - Still lots of memory allocation taking up around 20% of the runtime or more
//      - This is probably from all the String creations, each of which has heap allocated data

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

    let buf_reader = BufReader::new(measurements_file);
    let mut map = HashMap::new();

    buf_reader
        .lines()
        // .take(1_000_000)
        .for_each(|line| process_line(&line.unwrap(), &mut map));

    return format_output(&map);
}

fn process_line(line: &str, map: &mut HashMap<String, StationData>) {
    let (name, temp) = split_measurement_string(line);

    map
        .entry(name.to_owned())
        .or_insert(StationData::new())
        .add_temp(temp);
}

fn split_measurement_string(line: &str) -> (&str, f32) {
    let mut name_and_temp = line.split(';');
    let name = name_and_temp.next().unwrap();
    let temp: f32 = name_and_temp.next().unwrap().parse().unwrap();

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