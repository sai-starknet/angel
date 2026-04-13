use sai_felt::Felt;

/// Block context information
#[derive(Debug, Clone, Default)]
pub struct BlockContext {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
}
