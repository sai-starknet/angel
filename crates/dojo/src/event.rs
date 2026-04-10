use crate::decoder::{DojoDecoder, DojoRecordEvent, DojoTableEvent};
use crate::store::DojoStoreTrait;
use crate::DojoToriiResult;
pub use anyhow::Result as AnyResult;
use async_trait::async_trait;
use dojo_introspect::events::{
    EventEmitted, EventRegistered, EventUpgraded, ModelRegistered, ModelUpgraded,
    ModelWithSchemaRegistered, StoreDelRecord, StoreSetRecord, StoreUpdateMember,
    StoreUpdateRecord,
};
use dojo_introspect::DojoSchemaFetcher;
use introspect_types::FeltIds;
use torii_introspect::events::{CreateTable, DeleteRecords, InsertsFields, UpdateTable};
use torii_types::event::EventContext;

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelWithSchemaRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        decoder
            .register_table(&self.namespace, &self.name, self.schema, context)
            .await
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .register_table(&self.namespace, &self.name, schema, context)
            .await
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for EventRegistered
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = CreateTable;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .register_table(&self.namespace, &self.name, schema, context)
            .await
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for ModelUpgraded
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = UpdateTable;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .update_table(self.selector, schema, context)
            .await
            .map(Into::into)
    }
}

#[async_trait]
impl<Store, F> DojoTableEvent<Store, F> for EventUpgraded
where
    Store: DojoStoreTrait + Sync + Send,
    F: DojoSchemaFetcher + Sync + Send + 'static,
{
    type Msg = UpdateTable;
    async fn event_to_msg(
        self,
        context: EventContext,
        decoder: &DojoDecoder<Store, F>,
    ) -> DojoToriiResult<Self::Msg> {
        let schema = decoder.fetcher.schema(self.address).await?;
        decoder
            .update_table(self.selector, schema, context)
            .await
            .map(Into::into)
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreSetRecord {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreUpdateRecord {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let (columns, data) =
            decoder.with_table(self.selector, |table| table.parse_values(self.values))?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            self.entity_id,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for EventEmitted {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let primary: [u8; 32] = self.keys.hash().into();
        let (columns, data) = decoder.with_table(self.selector, |table| {
            table.parse_record(self.keys, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            columns,
            primary,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreUpdateMember {
    type Msg = InsertsFields;
    fn event_to_msg(self, decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        let data = decoder.with_table(self.selector, |table| {
            table.parse_field(self.member_selector, self.values)
        })?;
        Ok(InsertsFields::new_single(
            self.selector,
            vec![self.member_selector],
            self.entity_id,
            data,
        ))
    }
}

impl<Store, F> DojoRecordEvent<Store, F> for StoreDelRecord {
    type Msg = DeleteRecords;
    fn event_to_msg(self, _decoder: &DojoDecoder<Store, F>) -> DojoToriiResult<Self::Msg> {
        Ok(DeleteRecords::new(
            self.selector,
            vec![self.entity_id.into()],
        ))
    }
}
