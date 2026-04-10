use primitive_types::U256;
use starknet_types_raw::Felt;
use torii::etl::{EventBody, EventMsg, TypeId};
use torii::typed_body_impl;
use torii_common::utils::{felts_to_u256, U256ParseError};

pub const ERC20_TRANSFER_SELECTOR_TYPE_ID: TypeId = TypeId::new("erc20.transfer");
pub const ERC20_APPROVAL_SELECTOR_TYPE_ID: TypeId = TypeId::new("erc20.approval");
pub const ERC20_TYPE_ID: TypeId = TypeId::new("erc20");

#[derive(Debug, Clone, Copy)]
pub enum Erc20Msg {
    Transfer(TransferInfo),
    Approval(ApprovalMsg),
}

impl EventMsg for Erc20Msg {
    fn event_id(&self) -> String {
        match self {
            Erc20Msg::Transfer(_) => "erc20.transfer".to_string(),
            Erc20Msg::Approval(_) => "erc20.approval".to_string(),
        }
    }

    fn envelope_type_id(&self) -> TypeId {
        ERC20_TYPE_ID
    }
}

typed_body_impl!(Erc20Msg, "erc20");

pub type Erc20Body = EventBody<Erc20Msg>;

impl From<TransferInfo> for Erc20Msg {
    fn from(value: TransferInfo) -> Self {
        Erc20Msg::Transfer(value)
    }
}

impl From<ApprovalMsg> for Erc20Msg {
    fn from(value: ApprovalMsg) -> Self {
        Erc20Msg::Approval(value)
    }
}

/// Transfer event from ERC20 token
#[derive(Debug, Clone, Copy)]
pub struct TransferInfo {
    pub from: Felt,
    pub to: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
}

/// Approval event from ERC20 token
#[derive(Debug, Clone, Copy)]
pub struct ApprovalInfo {
    pub owner: Felt,
    pub spender: Felt,
    /// Amount as U256 (256-bit), properly representing ERC20 token amounts
    pub amount: U256,
}

pub struct Erc20Event(Felt, Felt, U256);

impl Erc20Event{
    fn transfer(self, event)
}

impl Erc20Event {
    pub fn new_from_felts(from: Felt, to: Felt, amount: &[Felt]) -> Result<Self, U256ParseError> {
        Ok(Self(from, to, felts_to_u256(amount)?))
    }

    pub fn transfer(self) -> Erc20Msg {
        Erc20Msg::Transfer(self.into())
    }

    pub fn approval(self) -> Erc20Msg {
        Erc20Msg::Approval(self.into())
    }
}

impl From<Erc20Event> for TransferInfo {
    fn from(value: Erc20Event) -> Self {
        TransferInfo {
            from: value.0,
            to: value.1,
            amount: value.2,
        }
    }
}

impl From<Erc20Event> for ApprovalMsg {
    fn from(value: Erc20Event) -> Self {
        ApprovalMsg {
            owner: value.0,
            spender: value.1,
            amount: value.2,
        }
    }
}
