use starknet_types_raw::Felt;
use xxhash_rust::xxh3::Xxh3;

pub fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}

const ALLOWED_TYPE_NAME_CHARS: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

fn parse_type_name(type_name: &str) -> String {
    fn parse_char(c: char) -> char {
        if ALLOWED_TYPE_NAME_CHARS.contains(c) {
            c
        } else {
            '_'
        }
    }
    type_name
        .chars()
        .take(31)
        .map(parse_char)
        .collect::<String>()
        .to_lowercase()
}

pub trait HasherExt {
    fn new_based<T: AsBytes + ?Sized>(base: &T) -> Self;
    fn type_name(&self, name: &str) -> String;
    fn tuple_name(&self) -> String;
    fn branch<T: AsBytes + ?Sized>(&self, name: &T) -> Self;
    fn branch_to_type_name<T: AsBytes + ?Sized>(&self, leaf: &T, name: &str) -> String;
}

impl HasherExt for Xxh3 {
    fn new_based<T: AsBytes + ?Sized>(base: &T) -> Self {
        let mut hash = Xxh3::new();
        let bytes = base.as_bytes();
        hash.update(&(bytes.len() as u32).to_le_bytes());
        hash.update(&bytes);
        hash
    }

    fn type_name(&self, name: &str) -> String {
        let hash = &format!("{:032x}", self.digest128())[..31];
        format!("{}_{}", parse_type_name(name), hash)
    }

    fn tuple_name(&self) -> String {
        self.type_name("tuple")
    }

    fn branch<T: AsBytes + ?Sized>(&self, name: &T) -> Xxh3 {
        let mut hasher = self.clone();
        let bytes = name.as_bytes();
        hasher.update(&(bytes.len() as u32).to_le_bytes());
        hasher.update(&bytes);
        hasher
    }

    fn branch_to_type_name<T: AsBytes + ?Sized>(&self, leaf: &T, name: &str) -> String {
        self.branch(leaf).type_name(name)
    }
}

pub trait AsBytes {
    fn as_bytes(&self) -> Vec<u8>;
}

impl AsBytes for Felt {
    fn as_bytes(&self) -> Vec<u8> {
        self.into()
    }
}

impl AsBytes for str {
    fn as_bytes(&self) -> Vec<u8> {
        str::as_bytes(self).to_vec()
    }
}

impl AsBytes for String {
    fn as_bytes(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl AsBytes for u32 {
    fn as_bytes(&self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}
