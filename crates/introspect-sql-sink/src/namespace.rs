use crate::error::{NamespaceError, NamespaceResult};
use crate::{DbError, DbResult};
use introspect_types::ResultInto;
use itertools::Itertools;
use starknet_types_core::felt::Felt;
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

pub enum NamespaceMode {
    None,
    Single(Arc<str>),
    Address,
    Named(HashMap<Felt, Arc<str>>),
    Addresses(HashSet<Felt>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableKey {
    namespace: NamespaceKey,
    id: Felt,
}

impl TableKey {
    pub fn new(namespace: NamespaceKey, id: Felt) -> Self {
        Self { namespace, id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NamespaceKey {
    None,
    Single(Arc<str>),
    Address(Felt),
    Named(Arc<str>),
}

pub fn felt_to_namespace(address: &Felt) -> String {
    format!("{address:063x}")
}

impl Hash for TableKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.namespace.hash(state);
        self.id.hash(state);
    }
}

impl Hash for NamespaceKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            NamespaceKey::Address(addr) => addr.hash(state),
            NamespaceKey::Named(name) => name.hash(state),
            NamespaceKey::Single(_) => {}
            NamespaceKey::None => {}
        }
    }
}

impl Display for NamespaceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NamespaceKey::Address(addr) => write!(f, "{addr:063x}"),
            NamespaceKey::Named(name) | NamespaceKey::Single(name) => name.fmt(f),
            NamespaceKey::None => Ok(()),
        }
    }
}

impl Display for TableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if !matches!(self.namespace, NamespaceKey::Single(_) | NamespaceKey::None) {
            write!(f, "{} ", self.namespace)?;
        }
        write!(f, "{:#063x}", self.id)
    }
}

impl From<String> for NamespaceMode {
    fn from(value: String) -> Self {
        NamespaceMode::Single(value.into())
    }
}

impl From<&str> for NamespaceMode {
    fn from(value: &str) -> Self {
        NamespaceMode::Single(value.into())
    }
}

impl From<HashMap<Felt, String>> for NamespaceMode {
    fn from(value: HashMap<Felt, String>) -> Self {
        NamespaceMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, &str); N]> for NamespaceMode {
    fn from(value: [(Felt, &str); N]) -> Self {
        NamespaceMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[(Felt, String); N]> for NamespaceMode {
    fn from(value: [(Felt, String); N]) -> Self {
        NamespaceMode::Named(
            value
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect::<HashMap<_, _>>(),
        )
    }
}

impl<const N: usize> From<[Felt; N]> for NamespaceMode {
    fn from(value: [Felt; N]) -> Self {
        NamespaceMode::Addresses(value.into_iter().collect())
    }
}

impl From<Vec<Felt>> for NamespaceMode {
    fn from(value: Vec<Felt>) -> Self {
        NamespaceMode::Addresses(value.into_iter().collect())
    }
}

fn felt_try_from_namespace(namespace: &str) -> NamespaceResult<Felt> {
    match namespace.len() == 63 {
        true => Felt::from_hex(namespace).err_into(),
        false => Err(NamespaceError::InvalidAddressLength(namespace.to_string())),
    }
}

impl NamespaceMode {
    pub fn namespaces(&self) -> Option<Vec<String>> {
        match self {
            NamespaceMode::None => Some(vec!["".to_string()]),
            NamespaceMode::Single(name) => Some(vec![name.to_string()]),
            NamespaceMode::Address => None,
            NamespaceMode::Named(map) => {
                Some(map.values().unique().map(ToString::to_string).collect())
            }
            NamespaceMode::Addresses(set) => Some(set.iter().map(felt_to_namespace).collect()),
        }
    }

    pub fn get_namespace_key(
        &self,
        namespace: String,
        owner: &Felt,
    ) -> NamespaceResult<NamespaceKey> {
        match self {
            NamespaceMode::None => Ok(NamespaceKey::None),
            NamespaceMode::Single(s) => match **s == *namespace {
                true => Ok(NamespaceKey::Single(s.clone())),
                false => Err(NamespaceError::NamespaceMismatch(namespace, s.to_string())),
            },
            NamespaceMode::Address => {
                felt_try_from_namespace(&namespace).map(NamespaceKey::Address)
            }
            NamespaceMode::Named(map) => match map.get(owner) {
                Some(s) if **s == *namespace => Ok(NamespaceKey::Named(s.clone())),
                Some(s) => Err(NamespaceError::NamespaceMismatch(namespace, s.to_string())),
                None => Err(NamespaceError::AddressNotFound(*owner, namespace)),
            },
            NamespaceMode::Addresses(set) => {
                let address = felt_try_from_namespace(&namespace)?;
                match set.contains(&address) {
                    true => Ok(NamespaceKey::Address(address)),
                    false => Err(NamespaceError::AddressNotFound(address, namespace)),
                }
            }
        }
    }

    pub fn get_key(&self, namespace: String, id: Felt, owner: &Felt) -> NamespaceResult<TableKey> {
        self.get_namespace_key(namespace, owner)
            .map(|k| TableKey::new(k, id))
    }

    pub fn to_namespace(&self, from_address: &Felt) -> DbResult<NamespaceKey> {
        match self {
            NamespaceMode::None => Ok(NamespaceKey::None),
            NamespaceMode::Single(name) => Ok(NamespaceKey::Single(name.clone())),
            NamespaceMode::Address => Ok(NamespaceKey::Address(*from_address)),
            NamespaceMode::Named(map) => match map.get(from_address) {
                Some(namespace) => Ok(NamespaceKey::Named(namespace.clone())),
                None => Err(DbError::NamespaceNotFound(*from_address)),
            },
            NamespaceMode::Addresses(set) => match set.contains(from_address) {
                true => Ok(NamespaceKey::Address(*from_address)),
                false => Err(DbError::NamespaceNotFound(*from_address)),
            },
        }
    }
}
