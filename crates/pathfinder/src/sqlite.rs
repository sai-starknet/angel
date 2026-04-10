use rusqlite::types::ValueRef;
use rusqlite::{params, Connection, Error as SqliteError, Row};
use starknet_types_raw::Felt;

pub type SqliteResult<T> = Result<T, SqliteError>;

#[derive(Debug)]
pub struct BlockEventsRow {
    pub block_number: u64,
    pub events: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct NumberAndHashRow(pub u64, pub Felt);

#[derive(Debug)]
pub struct BlockContextRow {
    pub number: u64,
    pub hash: Felt,
    pub parent_hash: Felt,
    pub timestamp: u64,
    pub event_count: u64,
}

#[derive(Debug)]
pub struct BlockHashAndEventCountRow {
    pub number: u64,
    pub hash: Felt,
    pub event_count: u64,
}

impl From<NumberAndHashRow> for (u64, Felt) {
    fn from(value: NumberAndHashRow) -> Self {
        (value.0, value.1)
    }
}

impl TryFrom<&Row<'_>> for BlockEventsRow {
    type Error = SqliteError;

    fn try_from(value: &Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            block_number: value.get::<_, i64>(0)? as u64,
            events: value.get(1)?,
        })
    }
}

impl TryFrom<&Row<'_>> for NumberAndHashRow {
    type Error = SqliteError;

    fn try_from(value: &Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self(value.get::<_, i64>(0)? as u64, value.get_felt(1)?))
    }
}

impl TryFrom<&Row<'_>> for BlockContextRow {
    type Error = SqliteError;

    fn try_from(value: &Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            number: value.get::<_, i64>(0)? as u64,
            hash: value.get_felt(1)?,
            parent_hash: value.get_felt(2)?,
            timestamp: value.get::<_, i64>(3)? as u64,
            event_count: value.get::<_, i64>(4)? as u64,
        })
    }
}

impl TryFrom<&Row<'_>> for BlockHashAndEventCountRow {
    type Error = SqliteError;

    fn try_from(value: &Row<'_>) -> Result<Self, Self::Error> {
        Ok(Self {
            number: value.get::<_, i64>(0)? as u64,
            hash: value.get_felt(1)?,
            event_count: value.get::<_, i64>(2)? as u64,
        })
    }
}

pub trait RowExt {
    fn get_felt(&self, idx: usize) -> SqliteResult<Felt>;
    fn get_felt_opt(&self, idx: usize) -> SqliteResult<Option<Felt>>;
}

impl RowExt for Row<'_> {
    fn get_felt(&self, idx: usize) -> SqliteResult<Felt> {
        match self.get_ref(idx)? {
            ValueRef::Blob(b) => Felt::from_be_bytes_slice(b).map_err(|e| {
                SqliteError::FromSqlConversionFailure(idx, rusqlite::types::Type::Blob, Box::new(e))
            }),
            _ => Err(SqliteError::InvalidColumnType(
                idx,
                "felt".into(),
                rusqlite::types::Type::Blob,
            )),
        }
    }

    fn get_felt_opt(&self, idx: usize) -> SqliteResult<Option<Felt>> {
        match self.get_ref(idx)? {
            ValueRef::Blob(b) => Ok(Some(Felt::from_be_bytes_slice(b).map_err(|e| {
                SqliteError::FromSqlConversionFailure(idx, rusqlite::types::Type::Blob, Box::new(e))
            })?)),
            ValueRef::Null => Ok(None),
            _ => Err(SqliteError::InvalidColumnType(
                idx,
                "felt".into(),
                rusqlite::types::Type::Blob,
            )),
        }
    }
}

pub trait SqliteExt {
    fn query_range<T>(
        &self,
        sql: &str,
        from_block: u64,
        to_block: u64,
        count: usize,
    ) -> SqliteResult<Vec<T>>
    where
        T: for<'a> TryFrom<&'a Row<'a>, Error = SqliteError>;

    fn get_block_events_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockEventsRow>> {
        self.query_range(
            "SELECT block_number, events FROM transactions WHERE block_number BETWEEN ? AND ? ORDER BY block_number",
            from_block,
            to_block,
            (to_block.saturating_sub(from_block) + 1) as usize,
        )
    }
    fn get_block_tx_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>>;
    fn get_block_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>> {
        self.query_range(
            "SELECT number, hash FROM block_headers WHERE number BETWEEN ? AND ? ORDER BY number",
            from_block,
            to_block,
            (to_block.saturating_sub(from_block) + 1) as usize,
        )
    }
    fn get_block_hash_rows_with_count(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockHashAndEventCountRow>> {
        self.query_range(
            "SELECT number, hash, event_count FROM block_headers WHERE number BETWEEN ? AND ? ORDER BY number",
            from_block,
            to_block,
            (to_block.saturating_sub(from_block) + 1) as usize,
        )
    }
    fn get_block_context_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<BlockContextRow>> {
        self.query_range("SELECT number, hash, parent_hash, timestamp, event_count FROM block_headers WHERE number BETWEEN ? AND ? ORDER BY number", from_block, to_block, (to_block.saturating_sub(from_block) + 1)  as usize)
    }
    fn get_number_of_events_for_blocks(&self, from_block: u64, to_block: u64) -> SqliteResult<u64>;
}

impl SqliteExt for Connection {
    fn query_range<T>(
        &self,
        sql: &str,
        from_block: u64,
        to_block: u64,
        count: usize,
    ) -> SqliteResult<Vec<T>>
    where
        T: for<'a> TryFrom<&'a Row<'a>, Error = SqliteError>,
    {
        let mut stmt = self.prepare_cached(sql)?;
        let mut rows = stmt.query(params![from_block, to_block])?;
        let mut result = Vec::with_capacity(count);
        while let Some(row) = rows.next()? {
            result.push(T::try_from(row)?);
        }
        Ok(result)
    }
    fn get_block_tx_hash_rows(
        &self,
        from_block: u64,
        to_block: u64,
    ) -> SqliteResult<Vec<NumberAndHashRow>> {
        let count = self
            .prepare_cached(
                "SELECT COUNT(*) FROM transaction_hashes WHERE block_number BETWEEN ? AND ?",
            )?
            .query_row(params![from_block, to_block], |r| r.get::<_, i64>(0))
            .map(|v| v as usize)?;
        self.query_range(
            "SELECT block_number, hash FROM transaction_hashes WHERE block_number BETWEEN ? AND ? ORDER BY block_number",
            from_block,
            to_block,
            count,
        )
    }

    fn get_number_of_events_for_blocks(&self, from_block: u64, to_block: u64) -> SqliteResult<u64> {
        self.prepare_cached(
            "SELECT SUM(event_count) FROM block_headers WHERE number BETWEEN ? AND ?",
        )?
        .query_row(params![from_block, to_block], |r| r.get::<_, i64>(0))
        .map(|v| v as u64)
    }
}
