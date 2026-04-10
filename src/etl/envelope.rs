//! This module contains the envelope for the ETL pipeline.

use std::any::Any;
use std::collections::HashMap;
use torii_types::event::EventContext;
use xxhash_rust::const_xxh3::xxh3_64;

/// Type identifier based on a string hash
/// This allows sinks to identify and downcast envelope bodies
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EnvelopeTypeId(u64);

impl EnvelopeTypeId {
    /// Creates a TypeId from a string at compile time.
    pub const fn new(type_name: &str) -> Self {
        EnvelopeTypeId(xxh3_64(type_name.as_bytes()))
    }

    /// Returns the TypeId as a u64.
    pub const fn as_u64(&self) -> u64 {
        self.0
    }
}

// Re-export as TypeId for convenience
pub type TypeId = EnvelopeTypeId;

/// Trait for typed envelope bodies
/// Sinks can downcast to concrete types using type_id
pub trait TypedBody: Send + Sync {
    fn envelope_type_id(&self) -> TypeId;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Helper macro to implement TypedBody
#[macro_export]
macro_rules! typed_body_impl {
    ($t:ty, $url:expr) => {
        impl $crate::etl::envelope::TypedBody for $t {
            fn envelope_type_id(&self) -> $crate::etl::envelope::TypeId {
                $crate::etl::envelope::TypeId::new($url)
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }
        }
    };
}

/// Envelope wraps transformed data with metadata
/// This is the core data structure that flows through the ETL pipeline
pub struct Envelope {
    /// Unique identifier for this envelope
    pub id: String,

    /// Type identifier for the body
    pub type_id: TypeId,

    /// The actual data (can be downcast by sinks)
    pub body: Box<dyn TypedBody>,

    /// Metadata that sinks can use for filtering
    pub metadata: HashMap<String, String>,

    /// Timestamp when this envelope was created
    pub timestamp: i64,
}

impl Envelope {
    /// Creates a new envelope.
    pub fn new(id: String, body: Box<dyn TypedBody>, metadata: HashMap<String, String>) -> Self {
        let type_id = body.envelope_type_id();
        Self {
            id,
            type_id,
            body,
            metadata,
            timestamp: chrono::Utc::now().timestamp(),
        }
    }

    /// Tries to downcast the body to a concrete type.
    pub fn downcast_ref<T: 'static>(&self) -> Option<&T> {
        self.body.as_any().downcast_ref::<T>()
    }

    /// Tries to downcast the body to a mutable concrete type.
    pub fn downcast_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.body.as_any_mut().downcast_mut::<T>()
    }
}

/// Debug implementation for Envelope.
impl std::fmt::Debug for Envelope {
    /// Formats the envelope for debugging.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Envelope")
            .field("id", &self.id)
            .field("type_id", &self.type_id)
            .field("metadata", &self.metadata)
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct EventBody<T> {
    pub context: EventContext,
    pub msg: T,
}

pub trait EventMsg: Send + Sync + 'static {
    fn event_id(&self) -> String;
    fn envelope_type_id(&self) -> TypeId;
    fn to_body(self, context: EventContext) -> EventBody<Self>
    where
        Self: Sized,
    {
        EventBody { context, msg: self }
    }
    fn to_envelope(self, context: EventContext) -> Envelope
    where
        Self: Sized,
    {
        Envelope::new(
            self.event_id(),
            Box::new(self.to_body(context)),
            HashMap::new(),
        )
    }
    fn to_envelopes(self, context: EventContext) -> Vec<Envelope>
    where
        Self: Sized,
    {
        vec![self.to_envelope(context)]
    }
    fn to_ok_envelopes<E>(self, context: EventContext) -> Result<Vec<Envelope>, E>
    where
        Self: Sized,
    {
        Ok(self.to_envelopes(context))
    }
}

impl<T> From<EventBody<T>> for (T, EventContext) {
    fn from(value: EventBody<T>) -> Self {
        (value.msg, value.context)
    }
}

impl<T> From<(T, EventContext)> for EventBody<T> {
    fn from(value: (T, EventContext)) -> Self {
        let (msg, context) = value;
        EventBody { msg, context }
    }
}

impl<'a, T> From<&'a EventBody<T>> for (&'a T, &'a EventContext) {
    fn from(value: &'a EventBody<T>) -> Self {
        (&value.msg, &value.context)
    }
}

impl<T: EventMsg + Send + Sync + 'static> From<EventBody<T>> for Envelope {
    fn from(value: EventBody<T>) -> Self {
        Envelope::new(
            value.msg.event_id(),
            Box::new(value),
            HashMap::new(), // You can add relevant metadata here
        )
    }
}

impl<T> TypedBody for EventBody<T>
where
    T: EventMsg + Send + Sync + 'static,
{
    fn envelope_type_id(&self) -> TypeId {
        self.msg.envelope_type_id()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}
