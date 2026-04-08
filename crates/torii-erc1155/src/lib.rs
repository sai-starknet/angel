//! ERC1155 Semi-Fungible Token Indexer Library for Torii
//!
//! This library provides components for indexing ERC1155 token transfers
//! on Starknet. Like ERC20, we only track transfer history - NOT balances.
//! Clients should query the chain for actual balances to avoid inaccuracies
//! from genesis allocations.
//!
//! # Components
//!
//! - [`Erc1155Decoder`]: Decodes ERC1155 TransferSingle, TransferBatch, ApprovalForAll, and URI events
//! - [`Erc1155Sink`]: Processes decoded events, stores in SQLite, and publishes updates
//! - [`Erc1155Storage`]: SQLite storage with efficient pagination
//! - [`Erc1155Service`]: gRPC service for queries and real-time subscriptions
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use torii_erc1155::{Erc1155Decoder, Erc1155Service, Erc1155Sink, Erc1155Storage};
//! use torii_erc1155::proto::erc1155_server::Erc1155Server;
//!
//! // Create storage
//! let pool = torii_sql::DbPoolOptions::new().connect_any("./erc1155.db").await?;
//! let storage = Arc::new(Erc1155Storage::new(pool, "./erc1155.db").await?);
//!
//! // Create gRPC service
//! let grpc_service = Erc1155Service::new(storage.clone());
//!
//! // Create sink
//! let sink = Erc1155Sink::new(storage).with_grpc_service(grpc_service.clone());
//!
//! // Create decoder
//! let decoder = Arc::new(Erc1155Decoder::new());
//!
//! // Add to gRPC router
//! let grpc_router = tonic::transport::Server::builder()
//!     .add_service(Erc1155Server::new(grpc_service));
//! ```

pub mod balance_fetcher;
pub mod decoder;
pub mod grpc_service;
pub mod handlers;
pub mod identification;
pub mod sink;
pub mod storage;
pub mod synthetic;

// Include generated protobuf code
pub mod proto {
    include!("generated/torii.sinks.erc1155.rs");
}

// File descriptor set for gRPC reflection (used by consumers for reflection setup)
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/erc1155_descriptor.bin");

// Re-export main types for convenience
pub use balance_fetcher::{Erc1155BalanceFetchRequest, Erc1155BalanceFetcher};
pub use decoder::{
    Erc1155Body, Erc1155Decoder, Erc1155Message, OperatorApproval, TransferData, UriUpdate,
    ERC1155_TYPE_ID,
};
pub use grpc_service::Erc1155Service;
pub use handlers::{Erc1155MetadataCommandHandler, Erc1155TokenUriCommandHandler};
pub use identification::Erc1155Rule;
pub use sink::Erc1155Sink;
pub use storage::{
    Erc1155BalanceAdjustment, Erc1155BalanceData, Erc1155Storage, TokenTransferData, TokenUriData,
    TransferCursor,
};
pub use synthetic::{SyntheticErc1155Config, SyntheticErc1155Extractor};
