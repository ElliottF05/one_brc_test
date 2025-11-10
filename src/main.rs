// # Rules and limits
// 
// - No external library deps
//
// - Format of measurements.txt:
//      - `<string: station name>;<double: measurement>`
//      - eg: "Hamburg;12.0"
// 
// - Station name: 
//      - non null UTF-8 string
//      - min length 1 character and max length 100 bytes 
//      - containing neither ; nor \n characters. 
//      - i.e. this could be 100 one-byte characters, or 50 two-byte characters, etc.
//
// - Temperature value: 
//      - non null double between -99.9 (inclusive) and 99.9 (inclusive)
//      - always with one fractional digit
//
// - There is a maximum of 10,000 unique station names
// - Line endings in the file are \n characters on all platforms
// - Rounding is round towards positive
//
// - Output format:
//      - `<station name>=<min>/<mean>/<max>`
//      - entries are comma+space seprated (separator = ", ")
//      - result is enclosed in curly braces "{<result here>}"
//
// - Running my code:
//      - Run as normal: `cargo run --release`
//
//      - Profiling:
//          - `cargo build --profile profiling`
//          - `samply record ./target/profiling/one_brc_test`

#![feature(portable_simd)]

mod misc;
mod v1;
mod v2;
mod v3;
mod v4;
mod v5;
mod v6;
mod v7;
mod v8;
mod v9;
mod v10;
mod v11;
mod v12;
mod v13;
mod v14;
mod v15;

use std::time::Instant;

use regex::Regex;

const MEASUREMENTS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/measurements.txt");
const CORRECT_RESULTS_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/correct_results.txt");

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() {
    #[cfg(feature = "dhat-heap")]
    let _profiler = dhat::Profiler::new_heap();

    let start = Instant::now();

    // misc::store_city_names();
    // misc::test_hash_function();
    // misc::find_seed();
    // return;

    // run the 1brc code
    let results = v15::run(MEASUREMENTS_PATH);

    println!("Run completed in: {:?} seconds", start.elapsed().as_secs_f32());

    // store results
    store_result(&results);

    // check the result
    check_correct(&results);
}


fn store_result(results: &str) {
    std::fs::write("my_results.txt", results).unwrap();
    println!("Results stored in \"my_results.txt\"");
}

fn check_correct(results: &str) {
    let correct = std::fs::read_to_string(CORRECT_RESULTS_PATH).unwrap();

    if results != correct {
        println!("ERROR, output does not match expected!");
        if results != results.trim() {
            println!("whitspace");
        }
    } else {
        println!("PASSED!");
        return;
    }

    let re = Regex::new(r"([^=]+)=([^,}]+)").unwrap();

    let results_groups: Vec<_> = re.captures_iter(&results)
        .map(|c| (c.get(1).unwrap().as_str(), c.get(2).unwrap().as_str()))
        .collect();
    
    let correct_groups: Vec<_> = re.captures_iter(&correct)
        .map(|c| (c.get(1).unwrap().as_str(), c.get(2).unwrap().as_str()))
        .collect();

    if results_groups.len() != correct_groups.len() {
        println!("Incorrect number of stations; expected {}, got {}!", correct_groups.len(), results_groups.len());
        return;
    }

    for i in 0..results_groups.len() {
        let (r_name, r_data) = results_groups[i];
        let (c_name, c_data) = correct_groups[i];

        if r_name != c_name {
            println!("Station names do not match, expected {}, got {}!", c_name, r_name);
        } else if r_data != c_data {
            println!("Station data does not match for station {}, expected {}, got {}!", c_name, c_data, r_data);
        }
    }
}