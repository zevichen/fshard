use std::hash::{BuildHasher, Hasher};

use crate::hash::ring::HashRing;

pub fn hash(value: &str) -> u32 {
    let mut hasher = FnvHasher::new();
    hasher.write(value.as_bytes());
    hasher.finish() as u32
}

pub fn compute(value: &str, algorithm: &str, shards: &u32, vms: &u32) -> u32 {
    let (index, name) = match algorithm {
        "h" => (fnv(value) % shards, "FNV1哈希取模"),
        "ch" => (hash_ring_get_key(value, shards, vms) as u32, "FNV1一致性哈希"),
        "j" => (latin1_java_hash_code(value) % shards, "Java latin1 HashCode"),
        _ => panic!("不支持该算法{}", algorithm)
    };
    println!("入参:{}, 算法:{}, 分片数:{}, 索引:{}", value, name, shards, index);
    index
}

pub fn hash_ring_get_key(value: &str, shards: &u32, vms: &u32) -> u32 {
    let mut ring = hash_ring(shards, vms);
    let mut data = ring.get(&value).expect("在哈希环中未找到对应的值");
    let vec = data.split("&").collect::<Vec<_>>();
    vec[0].parse::<u32>().expect("类型错误，无法将字符转换成整型")
}


/// 保持和Java中的HashUtil.fnv()一致
pub fn fnv(value: &str) -> u32 {
    fnv1(&value.as_bytes().to_vec())
}

pub fn fnv1(value: &Vec<u8>) -> u32 {
    let p = 16777619i32;
    let mut hash = 2166136261u32 as i32;
    for byte in value {
        hash = (hash ^ (*byte as i32)).wrapping_mul(p);
    }

    hash = hash.overflowing_add(&hash << 13).0;
    hash = hash ^ (&hash >> 7);
    hash = hash.overflowing_add(&hash << 3).0;
    hash = hash ^ (&hash >> 17);
    hash = hash.overflowing_add(&hash << 5).0;
    let i = hash.abs() as u32;
    //println!("fnv1.inner.computing={},value={:?},bytes={:?}", i, String::from_utf8(value.to_vec()).expect("不能转换为字符串"), value);
    i
}


/// 哈希环，32或64位
fn hash_ring(shards: &u32, vms: &u32) -> HashRing<FnvHashBuilder> {
    let mut ring = HashRing::with_hasher(FnvHashBuilder);
    for i in 0..*shards {
        for j in 0..*vms {
            let node = format!("{}&VN{}", i, j);
            ring.add(node);
        }
    }
    ring
}

#[derive(Debug)]
pub struct FnvHashBuilder;

impl BuildHasher for FnvHashBuilder {
    type Hasher = FnvHasher;

    fn build_hasher(&self) -> Self::Hasher {
        FnvHasher::new()
    }
}

pub struct FnvHasher {
    data: Vec<u8>,
}

impl FnvHasher {
    #[inline]
    pub fn new() -> Self {
        FnvHasher {
            data: vec![]
        }
    }
}

impl Hasher for FnvHasher {
    fn finish(&self) -> u64 {
        fnv1(&self.data) as u64
    }

    fn write(&mut self, bytes: &[u8]) {
        println!("hasher.write.fnv1={}", fnv1(&bytes.to_vec()));

        self.data = bytes.to_vec();
    }
}

pub fn latin1_java_hash_code(value: &str) -> u32 {
    let mut hash = 0i32;
    for x in value.as_bytes() {
        hash = hash.overflowing_mul(31).0.overflowing_add((x & 0xff) as i32).0;
    }
    hash.abs() as u32
}
