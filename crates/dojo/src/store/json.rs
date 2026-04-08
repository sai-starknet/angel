use super::DojoStoreTrait;
use crate::{DojoTable, DojoToriiResult};
use async_trait::async_trait;
use introspect_types::ResultInto;
use serde_json::Error as JsonError;
use starknet_types_core::felt::Felt;
use std::fs;
use std::path::PathBuf;
use torii_common::json::JsonFs;

pub struct JsonStore {
    pub path: PathBuf,
}

fn felt_to_fixed_hex_string(felt: &Felt) -> String {
    format!("0x{felt:0>32x}")
}
fn felt_to_json_file_name(felt: &Felt) -> String {
    format!("{}.json", felt_to_fixed_hex_string(felt))
}

impl<T: Into<PathBuf>> From<T> for JsonStore {
    fn from(path: T) -> Self {
        Self::new(path.into())
    }
}

impl JsonStore {
    pub fn new(path: PathBuf) -> Self {
        if !path.exists() {
            std::fs::create_dir_all(&path).expect("Unable to create directory");
        }

        // A temporary flag to clean the store on start, useful when debugging
        // to avoid existing table error.
        // TODO: @bengineer42 can be removed or kept being configurable.
        let clean_on_start = true;
        if clean_on_start {
            std::fs::remove_dir_all(&path).expect("Unable to clean directory");
            std::fs::create_dir_all(&path).expect("Unable to create directory");
        }

        Self { path }
    }

    pub fn dump_table(&self, table: &DojoTable) -> Result<(), JsonError> {
        self.path
            .join(felt_to_json_file_name(&table.id))
            .dump(table)
    }

    pub fn load_all_tables(&self) -> Result<Vec<DojoTable>, JsonError> {
        let mut tables: Vec<DojoTable> = Vec::new();
        let paths = fs::read_dir(&self.path).map_err(JsonError::io)?;
        for path in paths {
            tables.push(path.load()?);
        }
        Ok(tables)
    }

    pub fn load_all_as_map(&self) -> Result<Vec<(Felt, DojoTable)>, JsonError> {
        Ok(self
            .load_all_tables()?
            .into_iter()
            .map(|table| (table.id, table))
            .collect())
    }
}

#[async_trait]
impl DojoStoreTrait for JsonStore {
    async fn initialize(&self) -> DojoToriiResult {
        // No initialization needed for JSON store, but we can check if the path is accessible.
        if !self.path.exists() {
            std::fs::create_dir_all(&self.path).map_err(JsonError::io)?;
        }
        Ok(())
    }
    async fn save_table(
        &self,
        _owner: Felt,
        table: &DojoTable,
        _tx_hash: Felt,
        _block_number: u64,
    ) -> DojoToriiResult {
        self.dump_table(table).err_into()
    }

    async fn read_tables(&self, _owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        self.load_all_tables().err_into()
    }
}
