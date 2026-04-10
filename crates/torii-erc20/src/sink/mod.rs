use primitive_types::U256;
use starknet_types_raw::Felt;
use torii::etl::EventContext;

use crate::event::{ApprovalMsg, Erc20Body, TransferInfo};
use crate::Erc20Msg;

mod sqlite;

pub trait Erc20Store {}

#[derive(Debug, Clone)]
pub struct Transfer {
    pub token: Felt,
    pub from: Felt,
    pub to: Felt,
    pub amount: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone)]
pub struct Approval {
    pub token: Felt,
    pub owner: Felt,
    pub spender: Felt,
    pub amount: U256,
    pub block_number: u64,
    pub transaction_hash: Felt,
}

#[derive(Debug, Clone)]
pub enum Erc20Event {
    Transfer(Transfer),
    Approval(Approval),
}

impl From<Transfer> for Erc20Event {
    fn from(value: Transfer) -> Self {
        Erc20Event::Transfer(value)
    }
}

impl From<Approval> for Erc20Event {
    fn from(value: Approval) -> Self {
        Erc20Event::Approval(value)
    }
}

impl From<(ApprovalMsg, EventContext)> for Erc20Event {
    fn from(value: (ApprovalMsg, EventContext)) -> Self {
        Approval::from(value).into()
    }
}

impl From<(TransferInfo, EventContext)> for Erc20Event {
    fn from(value: (TransferInfo, EventContext)) -> Self {
        Transfer::from(value).into()
    }
}

impl From<(TransferInfo, EventContext)> for Transfer {
    fn from(value: (TransferInfo, EventContext)) -> Self {
        let (msg, context) = value;
        Transfer {
            token: context.from_address,
            from: msg.from,
            to: msg.to,
            amount: msg.amount,
            block_number: context.block_number,
            transaction_hash: context.transaction_hash,
        }
    }
}

impl From<(ApprovalMsg, EventContext)> for Approval {
    fn from(value: (ApprovalMsg, EventContext)) -> Self {
        let (msg, context) = value;
        Approval {
            token: context.from_address,
            owner: msg.owner,
            spender: msg.spender,
            amount: msg.amount,
            block_number: context.block_number,
            transaction_hash: context.transaction_hash,
        }
    }
}

impl From<Erc20Body> for Erc20Event {
    fn from(value: Erc20Body) -> Self {
        let (msg, context) = value.into();
        match msg {
            Erc20Msg::Transfer(transfer_msg) => (transfer_msg, context).into(),
            Erc20Msg::Approval(approval_msg) => (approval_msg, context).into(),
        }
    }
}
