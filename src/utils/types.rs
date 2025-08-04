use async_trait::async_trait;
use solana_sdk::{pubkey::Pubkey, signature::Signature};
use solana_transaction_status::{
    UiConfirmedBlock, EncodedTransactionWithStatusMeta,
};
use crate::utils::config::NetworkType;
use std::path::Path;
use std::path::PathBuf;

/// Type alias for a transaction ID, represented by a Solana `Signature`.
pub type TxId = Signature;

/// Type alias for an account identifier, represented by a Solana `Pubkey`.
pub type Account = Pubkey;

/// Type alias for a Solana transaction, specifically `VersionedTransactionWithStatusMeta`.
pub type RpcTransaction = EncodedTransactionWithStatusMeta;

/// Type alias for a block, represented by a Solana `VersionedConfirmedBlockWithEntries`.
pub type RpcBlock = UiConfirmedBlock;


/// Trait for custom client implementations
#[async_trait]
pub trait CustomClientTrait: Send + Sync {
    /// Get a block with entries
    async fn get_finalized_slots_between(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> anyhow::Result<Vec<u64>>;

    async fn get_block(
        &self,
        slot_number: u64
    ) -> anyhow::Result<RpcBlock>;

    async fn get_highest_snapshot_slot(&self) -> anyhow::Result<u64>;

    async fn get_and_download_latest_snapshot(
        &self,
        network: &NetworkType
    ) -> anyhow::Result<(String, u64)>;

    async fn download_file(url: &str, dest_path: &Path) -> anyhow::Result<String>;

    async fn get_and_download_blocks(
        &self,
        network: &NetworkType,
        highest_slot: u64,
        num_blocks: u64
    ) -> anyhow::Result<PathBuf>;

    async fn get_and_download_transactions(
        &self,
        network: &NetworkType,
        highest_slot: u64,
        num_blocks: u64
    ) -> anyhow::Result<(PathBuf, usize)> ;

      async fn extract_transactions(
        &self,
        network: &NetworkType, 
        blocks_path: &PathBuf
    ) -> anyhow::Result<(PathBuf, usize)>;

}
