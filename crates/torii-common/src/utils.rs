use primitive_types::U256;
use starknet_types_raw::Felt;

pub trait ElementsInto<T> {
    fn elements_into(self) -> Vec<T>;
}

pub trait ElementsFrom<T> {
    fn elements_from(vec: Vec<T>) -> Self;
}

impl<T, U> ElementsInto<U> for Vec<T>
where
    T: Into<U>,
{
    fn elements_into(self) -> Vec<U> {
        self.into_iter().map(T::into).collect()
    }
}

impl<T, U> ElementsFrom<T> for Vec<U>
where
    U: From<T>,
{
    fn elements_from(vec: Vec<T>) -> Self {
        vec.into_iter().map(U::from).collect()
    }
}

#[derive(Debug, Clone)]
pub enum U256ParseError {
    MoreThanTwo(usize),
    PairHighNonZero(Felt, Felt),
}

impl std::fmt::Display for U256ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            U256ParseError::MoreThanTwo(len) => write!(f, "Expected at most 2 felts for U256, got {}", len),
            U256ParseError::PairHighNonZero(low, high) => write!(
                f,
                "Expected high part of U256 to be zero when low part is non-zero. Got low: {}, high: {}",
                low, high
            ),
        }
    }
}

impl std::error::Error for U256ParseError {}

/// Parse a U256 result from balance_of return value
///
/// ERC20 balance_of typically returns:
/// - Cairo 0: A single felt (fits in 252 bits, usually enough for balances)
/// - Cairo 1 with u256: Two felts [low, high] representing a 256-bit value
pub fn felts_to_u256<F: AsRef<[Felt]>>(result: F) -> Result<U256, U256ParseError> {
    let result = result.as_ref();
    match result.len() {
        0 => Ok(U256::from(0u64)),
        1 => {
            // Single felt - convert to U256
            // Felt is 252 bits max, so it fits in the low part
            // Take the lower 16 bytes for u128 (fits any felt value)
            Ok(felt_to_u256(result[0]))
        }
        2 => {
            // Two felts: [low, high] for u256
            felt_pair_to_u256(result[0], result[1])
        }
        len => Err(U256ParseError::MoreThanTwo(len)),
    }
}

pub fn felt_pair_to_u256(low: Felt, high: Felt) -> Result<U256, U256ParseError> {
    let [l0, l1, d0, d1] = low.to_le_words();
    let [h0, h1, d2, d3] = high.to_le_words();
    match [d0, d1, d2, d3] {
        [0, 0, 0, 0] => Ok(U256([l0, l1, h0, h1])),
        _ => Err(U256ParseError::PairHighNonZero(low, high)),
    }
}

pub fn felt_to_u256(value: Felt) -> U256 {
    U256(value.to_le_words())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_u256_empty() {
        let result = felts_to_u256(&[]).unwrap();
        assert_eq!(result, U256::zero());
    }

    #[test]
    fn test_parse_u256_single_felt() {
        let felt = Felt::from(1000u64);
        let result = felts_to_u256(&[felt]).unwrap();
        assert_eq!(result, U256::from(1000u64));
    }

    #[test]
    fn test_parse_u256_two_felts() {
        // low = 100, high = 0
        let low = Felt::from(100u64);
        let high = Felt::from(0u64);
        let result = felts_to_u256(&[low, high]).unwrap();
        assert_eq!(result, U256::from(100u64));

        // Test with high value
        let low = Felt::from(0u64);
        let high = Felt::from(1u64);
        let result = felts_to_u256(&[low, high]).unwrap();
        // high = 1 means value = 1 * 2^128
        let expected = U256::from(1u64) << 128;
        assert_eq!(result, expected);
    }
}
