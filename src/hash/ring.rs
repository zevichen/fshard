use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::hash::BuildHasher;

use crate::hash::computing::{fnv, FnvHasher};

pub struct DefaultHashBuilder;

impl BuildHasher for DefaultHashBuilder {
    type Hasher = FnvHasher;

    fn build_hasher(&self) -> Self::Hasher {
        FnvHasher::new()
    }
}

// Node is an internal struct used to encapsulate the nodes that will be added and
// removed from `HashRing`
#[derive(Debug)]
struct Node {
    key: u64,
    value: String,
}

impl Node {
    fn new(key: u64, node: String) -> Node {
        Node { key, value: node }
    }
}

// Implement `PartialEq`, `Eq`, `PartialOrd` and `Ord` so we can sort `Node`s
impl PartialEq for Node {
    fn eq(&self, other: &Node) -> bool {
        self.key == other.key
    }
}

impl Eq for Node {}

impl PartialOrd for Node {
    fn partial_cmp(&self, other: &Node) -> Option<Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl Ord for Node {
    fn cmp(&self, other: &Node) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug)]
pub struct HashRing<S = DefaultHashBuilder> {
    hash_builder: S,
    ring: Vec<Node>,
}

impl Default for HashRing {
    fn default() -> Self {
        HashRing {
            hash_builder: DefaultHashBuilder,
            ring: Vec::new(),
        }
    }
}

/// Hash Ring
///
/// A computing ring that provides consistent hashing for nodes that are added to it.
impl HashRing {
    /// Create a new `HashRing`.
    pub fn new() -> HashRing {
        Default::default()
    }
}

impl<S> HashRing<S> {
    /// Creates an empty `HashRing` which will use the given computing builder.
    pub fn with_hasher(hash_builder: S) -> HashRing<S> {
        HashRing {
            hash_builder,
            ring: Vec::new(),
        }
    }

    /// Get the number of nodes in the computing ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns true if the ring has no elements.
    pub fn is_empty(&self) -> bool {
        self.ring.len() == 0
    }
}

impl<S: BuildHasher> HashRing<S> {
    /// Add `node` to the computing ring.
    pub fn add(&mut self, node: String) {
        let key = get_key(&node);
        self.ring.push(Node::new(key, node));
        self.ring.sort();
    }

    /// Remove `node` from the computing ring. Returns an `Option` that will contain the `node`
    /// if it was in the computing ring or `None` if it was not present.
    pub fn remove(&mut self, node: &str) -> Option<String> {
        let key = get_key(node);
        match self.ring.binary_search_by(|node| node.key.cmp(&key)) {
            Err(_) => None,
            Ok(n) => Some(self.ring.remove(n).value),
        }
    }

    /// Get the node responsible for `key`. Returns an `Option` that will contain the `node`
    /// if the computing ring is not empty or `None` if it was empty.
    pub fn get(&mut self, key: &str) -> Option<&str> {
        if self.ring.is_empty() {
            return None;
        }

        let k = get_key(key);
        let n = match self.ring.binary_search_by(|node| node.key.cmp(&k)) {
            Err(n) => n,
            Ok(n) => n,
        };

        if n == self.ring.len() {
            return Some(&self.ring[0].value);
        }

        Some(&self.ring[n].value)
    }
}

// An internal function for converting a reference to a `str` into a `u64` which
// can be used as a key in the computing ring.
fn get_key(input: &str) -> u64 {
    fnv(input) as u64
}