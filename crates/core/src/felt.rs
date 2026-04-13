use ahash::{HashMap, HashSet};
use sai_felt::Felt;

pub type FeltMap<V> = HashMap<Felt, V>;
pub type FeltSet = HashSet<Felt>;
