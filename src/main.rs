use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};

use clap::{App, Arg, SubCommand};

use crate::hash::computing::compute;

pub mod hash;

// use crate::fnv::FnvHasher;

// subjectId=37466027,shard=22
// algorith: ch,h
// sharding: 32,64...
// value: "",i32,i64,String
// number_of_shards,number_of_replicas
// shard key field,shard key value
// Hash参考文档：https://stackoverflow.com/questions/66905113/returning-a-hash-as-string-rust
fn main() {
    let matches = App::new("fshard")
        .version("1.0")
        .author("ZeviChen <zevichen@qq.com>")
        .about("find the sharding index.")
        .arg(Arg::with_name("VALUE")
            .help("Sets the sharding value")
            // fshard "zhangsan"，这个就是value
            .required(true)
            .index(1))
        .arg(Arg::with_name("algorithm")
            .short("a")
            .long("algorithm")
            .default_value("ch")
            .possible_values(&["ch", "h", "j"])
            .value_name("Enum")
            .help("Sets an algorithm to calculate the sharding value, ch=[一致性Hash算法], h=[Hash取模], j=[Java中的hashCode]")
            .takes_value(true))
        .arg(Arg::with_name("shards")
            .short("s")
            .long("shards")
            .default_value("64")
            .value_name("Number")
            .help("Sets the number of shards"))
        .arg(Arg::with_name("vms")
            .short("vms")
            .long("vms")
            .default_value("2")
            .value_name("Number")
            .help("Sets the number of virtual nodes"))
        .get_matches();

    let value = matches.value_of("VALUE").expect("The sharding value error");
    // println!("Using the sharding value: {}", value);
    let algorithm = matches.value_of("algorithm").unwrap_or_default();
    // println!("Value for algorithm: {}", algorithm);
    let shards = matches.value_of("shards").unwrap_or_default().parse::<u32>().expect("Nonnumeric error for shards");
    // println!("The number of shards: {}", shards);
    let vms = matches.value_of("vms").unwrap_or_default().parse::<u32>().expect("Nonnumeric error for vms");
    // println!("The number of vms: {}", vms);

    compute(value, algorithm, &shards, &vms);
}
