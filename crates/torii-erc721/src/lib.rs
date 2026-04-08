//! ERC721 NFT Token Indexer Library for Torii
//!
//! This library provides components for indexing ERC721 NFT transfers, approvals,
//! and ownership on Starknet. It can be used as a standalone sink or combined
//! with other token indexers in a unified binary.
//!
//! # Components
//!
//! - [`Erc721Decoder`]: Decodes ERC721 Transfer, Approval, and ApprovalForAll events
//! - [`Erc721Sink`]: Processes decoded events, stores them, and publishes updates
//! - [`Erc721Storage`]: DbPool-backed storage with ownership tracking and efficient pagination
//! - [`Erc721Service`]: gRPC service for queries and real-time subscriptions
//!
//! # Example
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use torii_erc721::{Erc721Decoder, Erc721Service, Erc721Sink, Erc721Storage};
//! use torii_erc721::proto::erc721_server::Erc721Server;
//!
//! // Create storage
//! let storage = Arc::new(Erc721Storage::new("./erc721.db")?);
//!
//! // Create gRPC service
//! let grpc_service = Erc721Service::new(storage.clone());
//!
//! // Create sink
//! let sink = Erc721Sink::new(storage).with_grpc_service(grpc_service.clone());
//!
//! // Create decoder
//! let decoder = Arc::new(Erc721Decoder::new());
//!
//! // Add to gRPC router
//! let grpc_router = tonic::transport::Server::builder()
//!     .add_service(Erc721Server::new(grpc_service));
//! ```

mod conversions;
pub mod decoder;
pub mod grpc_service;
pub mod handlers;
pub mod identification;
pub mod sink;
pub mod storage;
pub mod synthetic;

// Include generated protobuf code
pub mod proto {
    include!("generated/torii.sinks.erc721.rs");
}

// File descriptor set for gRPC reflection (used by consumers for reflection setup)
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!("generated/erc721_descriptor.bin");

// Re-export main types for convenience
pub use decoder::{
    BatchMetadataUpdate, Erc721Body, Erc721Decoder, Erc721Msg, MetadataUpdate, NftApproval,
    NftTransfer, OperatorApproval, ERC721_TYPE,
};
pub use grpc_service::Erc721Service;
pub use handlers::{Erc721MetadataCommandHandler, Erc721TokenUriCommandHandler};
pub use identification::Erc721Rule;
pub use sink::Erc721Sink;
pub use storage::{Erc721Storage, NftOwnershipData, NftTransferData, TransferCursor};
pub use synthetic::{SyntheticErc721Config, SyntheticErc721Extractor};
