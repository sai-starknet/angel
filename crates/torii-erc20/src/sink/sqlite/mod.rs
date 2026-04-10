use std::collections::HashMap;

use primitive_types::U256;
use starknet_types_raw::Felt;

pub struct CurrentState {
    pub token: Felt,
}

pub struct Requests {
    pub balances: Vec<(Felt, Felt)>,
    pub approvals: Vec<(Felt, Felt, Felt)>,
}

pub struct BalanceRequest {
    pub token: Felt,
    pub wallet: Felt,
}

pub struct ApprovalRequest {
    pub token: Felt,
    pub owner: Felt,
    pub spender: Felt,
}

pub trait Erc20Sqlite {
    fn get_balances(&self, balances: Vec<(Felt, Felt)>) -> Result<HashMap<(Felt, Felt), U256>, ()>;
    fn get_approvals(
        &self,
        approvals: Vec<(Felt, Felt, Felt)>,
    ) -> Result<HashMap<(Felt, Felt, Felt), U256>, ()>;
}



pub fn 