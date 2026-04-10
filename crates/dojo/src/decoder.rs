use crate::external_contract::{ExternalContractRegistered, ExternalContractRegisteredEvent};
use crate::store::DojoStoreTrait;
use crate::table::{sort_columns, DojoTableInfo};
use crate::{DojoTable, DojoToriiError, DojoToriiResult};
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect::selector::compute_selector_from_dojo_tag;
use dojo_introspect::serde::dojo_primary_def;
use dojo_introspect::{DojoSchema, DojoSchemaFetcher};
use introspect_types::{
    CairoEvent, CairoEventInfo, CairoSerde, IntoFeltSource, PrimaryDef, PrimaryTypeDef, ResultInto,
    SliceFeltSource,
};
use itertools::Itertools;
use starknet_types_raw::Felt;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::RwLock;
use torii::etl::{Decoder, Envelope, EventMsg};
use torii_introspect::events::{IntrospectBody, IntrospectMsg};
use torii_introspect::schema::TableSchema;
use torii_introspect::{CreateTable, EventId};
use torii_types::event::EventContext;

pub const DOJO_ID_FIELD_NAME: &str = "entity_id";

pub struct DojoDecoder<Store, F> {
    pub tables: RwLock<HashMap<Felt, DojoTableInfo>>,
    pub store: Store,
    pub fetcher: F,
    pub append_only: HashSet<(Felt, Felt)>,
}

fn deserialize_data<'a, T>(keys: &[Felt], data: &'a [Felt]) -> DojoToriiResult<T>
where
    T: CairoEvent<CairoSerde<SliceFeltSource<'a>>> + CairoEventInfo,
{
    let mut keys = keys.into_source();
    let mut data: CairoSerde<_> = data.into();
    match T::deserialize_and_verify_event(&mut keys, &mut data) {
        Ok(event) => Ok(event),
        Err(err) => Err(DojoToriiError::EventDeserializationError(T::NAME, err)),
    }
}

#[async_trait]
pub trait DojoTableEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg>;
}

pub trait DojoRecordEvent<Store, F>: Sized + CairoEventInfo + Debug {
    type Msg: EventId;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg>;
}

#[async_trait]
impl<Store, F> DojoStoreTrait for DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Sync,
    F: Sync,
{
    async fn initialize(&self) -> DojoToriiResult<()> {
        self.store.initialize().await
    }
    async fn save_table(
        &self,
        owner: Felt,
        table: &DojoTable,
        tx_hash: Felt,
        block_number: u64,
    ) -> DojoToriiResult<()> {
        self.store
            .save_table(owner, table, tx_hash, block_number)
            .await
    }

    async fn read_tables(&self, owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
        self.store.read_tables(owners).await
    }
}

pub fn primary_field_def() -> PrimaryDef {
    PrimaryDef {
        name: DOJO_ID_FIELD_NAME.to_string(),
        attributes: vec![],
        type_def: PrimaryTypeDef::Felt252,
    }
}

impl<Store, F> DojoDecoder<Store, F> {
    pub fn with_table<R>(
        &self,
        id: Felt,
        f: impl FnOnce(&DojoTableInfo) -> DojoToriiResult<R>,
    ) -> DojoToriiResult<R> {
        let tables = self.tables.read()?;
        let table = tables
            .get(&id)
            .ok_or_else(|| DojoToriiError::TableNotFoundById(id))?;
        f(table)
    }
}

impl<Store, F> DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Send + Sync + 'static,
{
    pub fn new(store: Store, fetcher: F) -> Self {
        Self {
            tables: Default::default(),
            store,
            fetcher,
            append_only: HashSet::new(),
        }
    }
    pub fn append_historical(&mut self, models: HashSet<(Felt, String)>) {
        for (address, name) in models {
            let table_id = compute_selector_from_dojo_tag(&name).unwrap();
            self.append_only.insert((address, table_id));
        }
    }
    pub async fn load_tables(&self, owners: &[Felt]) -> DojoToriiResult<()> {
        let new = self.read_tables(owners).await?;
        let mut tables = self.tables.write()?;
        tables.extend(new.into_iter().map_into());
        Ok(())
    }

    pub fn get_dojo_tables(&self) -> DojoToriiResult<Vec<DojoTable>> {
        let tables = self.tables.read()?;
        Ok(tables.iter().map_into().collect())
    }

    pub fn get_tables(&self) -> DojoToriiResult<Vec<TableSchema>> {
        Ok(self.get_dojo_tables()?.into_iter().map_into().collect())
    }

    pub fn with_tables<S: Into<Store>>(store: S, fetcher: F, tables: Vec<DojoTable>) -> Self {
        let store = store.into();
        let table_map = tables.into_iter().map(Into::into).collect();
        Self::with_table_map(store, fetcher, table_map)
    }

    pub fn with_table_map(store: Store, fetcher: F, tables: HashMap<Felt, DojoTableInfo>) -> Self {
        Self {
            tables: RwLock::new(tables),
            store,
            fetcher,
            append_only: HashSet::new(),
        }
    }

    pub async fn register_table(
        &self,
        namespace: &str,
        name: &str,
        schema: DojoSchema,
        context: EventContext,
    ) -> DojoToriiResult<CreateTable> {
        let full_table = DojoTable::from_schema(schema, namespace, name, dojo_primary_def());
        self.save_table(
            context.from_address,
            &full_table,
            context.transaction_hash,
            context.block_number,
        )
        .await?;
        let (id, table) = full_table.clone().into();
        {
            if let Some(existing) = self.tables.read()?.get(&id) {
                return Err(DojoToriiError::TableAlreadyExists(
                    id,
                    existing.name.clone(),
                    name.to_string(),
                ));
            }
        }
        self.tables.write()?.insert(id, table);
        Ok(CreateTable::from_schema(
            full_table.into(),
            self.append_only.contains(&(context.from_address, id)),
        ))
    }

    pub async fn update_table(
        &self,
        id: Felt,
        schema: DojoSchema,
        context: EventContext,
    ) -> DojoToriiResult<TableSchema> {
        let mut info = {
            let mut tables = self.tables.write()?;
            match tables.remove(&id) {
                Some(t) => t,
                None => return Err(DojoToriiError::TableNotFoundById(id)),
            }
        };
        let (columns, key_fields, value_fields) = sort_columns(schema.columns);
        info.columns.extend(columns);
        info.key_fields = key_fields;
        info.value_fields = value_fields;
        let table = (id, info).into();
        self.save_table(
            context.from_address,
            &table,
            context.transaction_hash,
            context.block_number,
        )
        .await?;
        let (_, info) = table.clone().into();
        self.tables.write()?.insert(id, info);
        Ok(table.to_schema())
    }

    async fn process_table_event<'a, E>(
        &self,
        keys: &'a [Felt],
        data: &'a [Felt],
        context: EventContext,
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoTableEvent<Store, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        deserialize_data::<E>(keys, data)?
            .event_to_msg(context, self)
            .await
            .ok_into()
    }

    fn process_record_event<'a, E>(
        &self,
        keys: &'a [Felt],
        data: &'a [Felt],
    ) -> DojoToriiResult<IntrospectMsg>
    where
        E: DojoRecordEvent<Store, F> + CairoEvent<CairoSerde<SliceFeltSource<'a>>> + Send,
        E::Msg: Into<IntrospectMsg>,
    {
        deserialize_data::<E>(keys, data)?
            .event_to_msg(self)
            .ok_into()
    }

    fn process_external_contract_event<'a>(
        &self,
        keys: &'a [Felt],
        values: &'a [Felt],
    ) -> DojoToriiResult<ExternalContractRegistered> {
        deserialize_data::<ExternalContractRegisteredEvent>(keys, values).map(Into::into)
    }

    pub async fn event_with_selector_to_msg(
        &self,
        selector: Felt,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> DojoToriiResult<IntrospectMsg> {
        match selector {
            ModelRegistered::SELECTOR => {
                self.process_table_event::<ModelRegistered>(keys, data, context)
                    .await
            }
            ModelWithSchemaRegistered::SELECTOR => {
                self.process_table_event::<ModelWithSchemaRegistered>(keys, data, context)
                    .await
            }
            ModelUpgraded::SELECTOR => {
                self.process_table_event::<ModelUpgraded>(keys, data, context)
                    .await
            }
            EventRegistered::SELECTOR => {
                self.process_table_event::<EventRegistered>(keys, data, context)
                    .await
            }
            EventUpgraded::SELECTOR => {
                self.process_table_event::<EventUpgraded>(keys, data, context)
                    .await
            }
            StoreSetRecord::SELECTOR => self.process_record_event::<StoreSetRecord>(keys, data),
            StoreUpdateRecord::SELECTOR => {
                self.process_record_event::<StoreUpdateRecord>(keys, data)
            }
            StoreUpdateMember::SELECTOR => {
                self.process_record_event::<StoreUpdateMember>(keys, data)
            }
            StoreDelRecord::SELECTOR => self.process_record_event::<StoreDelRecord>(keys, data),
            EventEmitted::SELECTOR => self.process_record_event::<EventEmitted>(keys, data),
            _ => Err(DojoToriiError::UnknownDojoEventSelector(selector)),
        }
    }

    pub async fn event_with_selector_to_body(
        &self,
        selector: Felt,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> DojoToriiResult<IntrospectBody> {
        self.event_with_selector_to_msg(selector, keys, data, context)
            .await
            .map(|msg| msg.to_body(context))
    }

    pub async fn event_to_msg(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> DojoToriiResult<IntrospectMsg> {
        let (selector, keys) = keys
            .split_first()
            .ok_or(DojoToriiError::MissingEventSelector)?;
        self.event_with_selector_to_msg(*selector, keys, data, context)
            .await
    }

    pub async fn event_to_body(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> DojoToriiResult<IntrospectBody> {
        self.event_to_msg(keys, data, context)
            .await
            .map(|msg| msg.to_body(context))
    }
}

#[async_trait]
impl<Store, F> Decoder for DojoDecoder<Store, F>
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    fn decoder_name(&self) -> &'static str {
        "dojo-introspect"
    }

    async fn decode(
        &self,
        keys: &[Felt],
        data: &[Felt],
        context: EventContext,
    ) -> AnyResult<Vec<Envelope>> {
        let (&selector, keys) = keys
            .split_first()
            .ok_or(DojoToriiError::MissingEventSelector)?;

        if selector == ExternalContractRegisteredEvent::SELECTOR {
            return self
                .process_external_contract_event(keys, data)
                .map(|msg| msg.to_envelopes(context))
                .err_into();
        }

        match self
            .event_with_selector_to_msg(selector, keys, data, context)
            .await
        {
            Ok(msg) => msg.to_ok_envelopes(context),
            Err(DojoToriiError::UnknownDojoEventSelector(_)) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ExternalContractRegisteredBody;
    use async_trait::async_trait;
    use dojo_introspect::DojoIntrospectError;
    use introspect_types::utils::string_to_cairo_serialize_byte_array;
    use introspect_types::{Attribute, ColumnDef, TypeDef};
    use std::sync::Mutex;
    use torii::etl::StarknetEvent;

    #[derive(Default)]
    struct FakeStore {
        saved_blocks: Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl DojoStoreTrait for FakeStore {
        async fn initialize(&self) -> DojoToriiResult {
            Ok(())
        }
        async fn save_table(
            &self,
            _owner: Felt,
            _table: &DojoTable,
            _tx_hash: Felt,
            block_number: u64,
        ) -> DojoToriiResult {
            self.saved_blocks.lock().unwrap().push(block_number);
            Ok(())
        }

        async fn read_tables(&self, _owners: &[Felt]) -> DojoToriiResult<Vec<DojoTable>> {
            Ok(Vec::new())
        }
    }

    struct PanicFetcher;

    #[async_trait]
    impl DojoSchemaFetcher for PanicFetcher {
        async fn schema(&self, _contract_address: Felt) -> Result<DojoSchema, DojoIntrospectError> {
            panic!("schema fetch should not be used in this test");
        }
    }

    fn schema(columns: &[(u64, &str, bool)]) -> DojoSchema {
        DojoSchema {
            name: "Duelist".to_string(),
            attributes: vec![Attribute::new_empty("model".to_string())],
            columns: columns
                .iter()
                .map(|(id, name, key)| ColumnDef {
                    id: Felt::from(*id),
                    name: (*name).to_string(),
                    attributes: if *key {
                        vec![Attribute::new_empty("key".to_string())]
                    } else {
                        vec![]
                    },
                    type_def: TypeDef::U32,
                })
                .collect(),
            legacy: false,
        }
    }

    #[tokio::test]
    async fn update_table_keeps_table_in_decoder_and_records_block() {
        let owner = Felt::from(0x123_u64);
        let initial = DojoTable::from_schema(
            schema(&[(1, "entity_id", true), (2, "health", false)]),
            "pistols",
            "Duelist",
            primary_field_def(),
        );
        let table_id = initial.id;
        let store = FakeStore::default();
        let decoder: DojoDecoder<FakeStore, PanicFetcher> =
            DojoDecoder::with_tables(store, PanicFetcher, vec![initial]);

        decoder
            .update_table(
                table_id,
                schema(&[
                    (1, "entity_id", true),
                    (2, "health", false),
                    (3, "armor", false),
                ]),
                EventContext {
                    from_address: owner,
                    transaction_hash: Felt::ZERO,
                    block_number: 42,
                },
            )
            .await
            .unwrap();

        let parsed = decoder
            .with_table(table_id, |table| Ok(table.columns.len()))
            .unwrap();
        assert_eq!(parsed, 3);
        assert_eq!(*decoder.store.saved_blocks.lock().unwrap(), vec![42]);
    }

    #[tokio::test]
    async fn decode_external_contract_registered_event_emits_control_envelope() {
        let decoder: DojoDecoder<FakeStore, PanicFetcher> =
            DojoDecoder::with_tables(FakeStore::default(), PanicFetcher, Vec::new());

        let mut keys = vec![ExternalContractRegisteredEvent::SELECTOR];
        keys.extend(string_to_cairo_serialize_byte_array("tokens"));
        keys.extend(string_to_cairo_serialize_byte_array("ERC20"));
        keys.extend(string_to_cairo_serialize_byte_array("eth"));
        keys.push(Felt::from_hex("0x99").unwrap());

        let event = StarknetEvent {
            from_address: Felt::from_hex("0x1").unwrap(),
            keys,
            data: vec![
                Felt::from_hex("0xabc").unwrap(),
                Felt::from_hex("0x1234").unwrap(),
                Felt::from(42_u64),
            ],
            block_number: 42,
            transaction_hash: Felt::from_hex("0xbeef").unwrap(),
        };

        let envelopes = decoder.decode_event(&event).await.unwrap();
        assert_eq!(envelopes.len(), 1);
        let body = envelopes[0]
            .downcast_ref::<ExternalContractRegisteredBody>()
            .unwrap();

        assert_eq!(body.msg.namespace, "tokens");
        assert_eq!(body.msg.contract_name, "ERC20");
        assert_eq!(body.msg.instance_name, "eth");
        assert_eq!(body.msg.contract_selector, Felt::from_hex("0x99").unwrap());
        assert_eq!(body.msg.class_hash, Felt::from_hex("0xabc").unwrap());
        assert_eq!(body.msg.contract_address, Felt::from_hex("0x1234").unwrap());
        assert_eq!(body.msg.registration_block, 42);
        assert_eq!(body.context.from_address, Felt::from_hex("0x1").unwrap());
    }
}
