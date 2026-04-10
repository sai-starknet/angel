//! ERC20 Token Indexer Library for Torii
//!
//! This library provides components for indexing ERC20 token transfers and approvals
//! on Starknet. It can be used as a standalone sink or combined with other token
//! indexers in a unified binary.
//!
//! # Components
//!
//! - [`Erc20Decoder`]: Decodes ERC20 Transfer and Approval events
//! - [`Erc20Sink`]: Processes decoded events, stores in SQLite, and publishes updates
//! - [`Erc20Storage`]: SQLite storage with efficient BLOB encoding and cursor pagination
//! - [`Erc20Service`]: gRPC service for queries and real-time subscriptions
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use torii_erc20::{Erc20Decoder, Erc20Service, Erc20Sink, Erc20Storage};
//! use torii_erc20::proto::erc20_server::Erc20Server;
//!
//! // Create storage
//! let storage = Arc::new(Erc20Storage::new("./erc20.db")?);
//!
//! // Create gRPC service
//! let grpc_service = Erc20Service::new(storage.clone());
//!
//! // Create sink
//! let sink = Erc20Sink::new(storage).with_grpc_service(grpc_service.clone());
//!
//! // Create decoder
//! let decoder = Arc::new(Erc20Decoder::new());
//!
//! // Add to gRPC router
//! let grpc_router = tonic::transport::Server::builder()
//!     .add_service(Erc20Server::new(grpc_service));
//! ```

pub mod balance_fetcher;
pub mod decoder;
pub mod event;
// pub mod grpc_service;
// pub mod handlers;
// pub mod identification;
// pub mod sink;
pub mod sink;
pub mod storage;
// pub mod synthetic;

// Include generated protobuf code
pub mod proto {
    include!("generated/torii.sinks.erc20.rs");
}

// File descriptor set for gRPC reflection (used by consumers for reflection setup)
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/erc20_descriptor.bin");

// Re-export main types for convenience
pub use balance_fetcher::{BalanceFetchRequest, BalanceFetcher};
pub use decoder::Erc20Decoder;
pub use event::{
    Erc20Msg, ERC20_APPROVAL_SELECTOR_TYPE_ID, ERC20_TRANSFER_SELECTOR_TYPE_ID, ERC20_TYPE_ID,
};
// pub use grpc_service::Erc20Service;
// pub use handlers::Erc20MetadataCommandHandler;
// pub use identification::Erc20Rule;
// pub use sink::Erc20Sink;
// pub use storage::{
//     ApprovalCursor, ApprovalData, BalanceAdjustment, BalanceData, Erc20Storage, TransferCursor,
//     TransferData, TransferDirection,
// };
// pub use synthetic::{SyntheticErc20Config, SyntheticErc20Extractor};
