use  crate::utils::types::{ CustomClientTrait};
use async_trait::async_trait;
use bzip2::read::BzDecoder;
use tar::Archive;
use std::fs;
use std::path::Path;
use anyhow::{Context,Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::io::Write;
use crate::utils::types::{RpcBlock};
use crate::utils::config::{load_json_config, upload_json_config, NetworkType};
use std::path::PathBuf;
use tracing::info;
use std::io;
use solana_transaction_status::UiTransactionEncoding;
use solana_transaction_status::TransactionDetails;


pub struct SolanaClient {
    client: RpcClient
}


impl SolanaClient {
    pub fn new(node_url: String) -> Self {
        let client = RpcClient::new_with_commitment(
            node_url,
            CommitmentConfig::confirmed(),
        );
        Self{
            client
        }
    }
}

#[async_trait]
impl CustomClientTrait for SolanaClient {
    /// Get finalized slots between a start and end slot using getBlocks RPC
    async fn get_finalized_slots_between(
        &self,
        start_slot: u64,
        end_slot: u64,
    ) -> anyhow::Result<Vec<u64>> {
        self.client.get_blocks(start_slot, Some(end_slot)).await.context("Failed to get requested blocks")
    }

    async fn get_block(
        &self,
        slot_number: u64
    ) -> anyhow::Result<RpcBlock> {
    
        let config = solana_client::rpc_config::RpcBlockConfig {
            encoding: UiTransactionEncoding::Base64.into(),
            transaction_details: TransactionDetails::Full.into(),
            rewards: None,
            commitment: CommitmentConfig::finalized().into(),
            max_supported_transaction_version: Some(0),
        };
        self.client.get_block_with_config(slot_number, config).await.context("Failed to get block with config")
    }

    async fn get_highest_snapshot_slot(&self) -> anyhow::Result<u64>{
        let snapshot_slot = self.client
            .get_highest_snapshot_slot()
            .await
            .context("Failed to get highest snapshot slot from RPC")?;

        Ok(snapshot_slot.full)
    }

    /// Gets the highest snapshot slot, constructs URL, downloads and returns info.
    /// Panics on RPC or download errors.
    async fn get_and_download_latest_snapshot(
        &self,
        network: &NetworkType
    ) -> Result<(String, u64)> {
        info!("Fetching highest snapshot slot...");
        let snapshot_slot = self.client
            .get_highest_snapshot_slot()
            .await
            .context("Failed to get highest snapshot slot from RPC")?;
        
        info!(
            slot = snapshot_slot.full,
            "Found highest full snapshot slot"
        );
        
        // Use the base URL for the initial request
        let base_url = self.client.url();
        let base_url = base_url.trim_end_matches('/');
        // Construct the base download URL - assuming it's /snapshot.tar.bz2
        let base_download_url = format!("{}/snapshot.tar.bz2", base_url);
        info!(url = %base_download_url, "Constructed base snapshot download URL");

        let network_key = network.to_string();
        let cache_dir = PathBuf::from("./cache");
        let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
        fs::create_dir_all(&snapshot_dir).context("Failed to create snapshot directory")?;

        // Define a temporary path for the download
        let temp_download_path = snapshot_dir.join("snapshot.download.tmp");

        info!(url = %base_download_url, path = ?temp_download_path, "Attempting download to temporary file...");
        // Download using the base URL, get the actual filename from redirect
        let actual_filename = SolanaClient::download_file(&base_download_url, &temp_download_path).await?;
        info!(path = ?temp_download_path, actual_filename = %actual_filename, "Snapshot download complete to temporary file.");

        // Calculate final path using the actual filename
        let final_path = snapshot_dir.join(&actual_filename);

        // Check if the final destination already exists
        if final_path.exists() {
            info!(path = ?final_path, "Final snapshot file already exists. Removing temporary file.");
            fs::remove_file(&temp_download_path).context("Failed to remove temporary download file")?;
        } else {
            info!(from = ?temp_download_path, to = ?final_path, "Renaming temporary file to final snapshot name.");
            fs::rename(&temp_download_path, &final_path).context(format!(
                "Failed to rename temporary file {:?} to final path {:?}",
                temp_download_path, final_path
            ))?;
        }

        info!("Updating config file with latest snapshot");
        let mut config = load_json_config("config.json").context("Could not get config file").unwrap();
        let network_config = config.networks.get_mut(&network_key).unwrap();
        network_config.start_snapshot = actual_filename.clone();
        upload_json_config("config.json", config)?; 

        info!("Preparing to download and extract genesis binary");
        let base_url = self.client.url();
        let base_url = base_url.trim_end_matches('/');
        // Construct the base download URL - assuming it's /snapshot.tar.bz2
        let base_download_url = format!("{}/genesis.tar.bz2", base_url);
        info!(url = %base_download_url, "Constructed genesis download URL");
        info!("Downloading genesis binary archive from {base_download_url}");
        let temp_download_path = PathBuf::from("./genesis").join(network_key.clone()).join("genesis.tar.bz2");
        Self::download_file(&base_download_url, temp_download_path.as_path()).await?;
        info!("Extracting genesis binary from archive");
        let decompressor = BzDecoder::new(fs::File::open(temp_download_path).unwrap());
        let mut archive = Archive::new(decompressor);
        for (_, file) in archive.entries().unwrap().enumerate() {
            let mut file = file.unwrap();
            let file_path = file.path()?;
            if file_path.to_str().unwrap().contains("genesis.bin")
            {
                file.unpack(PathBuf::from("./genesis").join(network_key.clone()).join("genesis.bin"))?;
            }
        }
        fs::remove_file(PathBuf::from("./genesis").join(network_key.clone()).join("genesis.tar.bz2"))?;
        
        Ok((String::new(), snapshot_slot.full)) // Use the filename obtained from the redirect 
    }

    /// Helper to download a file from a URL. Returns the final filename after redirects.
    /// Panics on error.
    async fn download_file(url: &str, dest_path: &Path) -> Result<String> {
        let response = reqwest::get(url)
        .await
        .context(format!("Failed to initiate download from {}", url))?;

        // Get the final URL after redirects
        let final_url = response.url().clone();
        info!(final_url = %final_url, "Final URL");

        // Extract filename from the final URL's path
        let final_filename = final_url.path_segments().
        and_then(|segments| segments.last()).
        ok_or_else(|| anyhow::anyhow!("Could not extract filename from final URL: {}", final_url))?.
        to_string();

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Download failed: URL {} (final: {}) returned status {}",
                url,
                final_url,
                response.status()
            ));
        }

        // Ensure parent directory exists
        if let Some(parent) = dest_path.parent() {
            fs::create_dir_all(parent).context(format!("Failed to create parent directory {:?}", parent))?;
        }

        let mut dest_file = fs::File::create(dest_path).context(format!("Failed to create destination file {:?}", dest_path))?;

        let content = response.bytes().await.context("Failed to read response bytes")?;

        io::copy(&mut content.as_ref(), &mut dest_file).context("Failed to write downloaded content to file")?;

        info!(path = ?dest_path, final_filename = %final_filename, "Temporary download complete.");
        Ok(final_filename)
    }
 
    async fn get_and_download_blocks(
        &self,
        network: &NetworkType,
        highest_slot: u64,
        num_blocks: u64
    ) -> anyhow::Result<PathBuf> {
        info!("Fetching {} blocks...", num_blocks);
                
        let network_key = network.to_string();
        let cache_dir = PathBuf::from("./cache");
        let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
        fs::exists(&snapshot_dir).context("Failed to get snapshot directory")?;
        info!("Found snapshot dir {:?}", snapshot_dir);

        // Calculate final path using the actual filename
        let blocks_path = snapshot_dir.join("blocks.json");

        // Check if the final destination already exists
        if blocks_path.exists() {
            info!("Found existing blocks file. Removing it to download new set of blocks.");
            fs::remove_file(blocks_path.as_path()).context("Failed to delete existing block file")?;
       } 
        
        info!("Creating new blocks json file at {:?}", blocks_path);
        let mut blocks_file = fs::File::create(blocks_path.as_path()).context("Failed to create blocks file")?;    
        
        let block_ids = self.get_finalized_slots_between(highest_slot - num_blocks, highest_slot).await?;
        let mut blocks = vec![];
        for id in block_ids {
            blocks.push(self.get_block(id).await.unwrap());
        }

        let json_content = serde_json::to_vec(&blocks).unwrap();
        blocks_file.write_all(&json_content)?;

        info!("Updating config file with num of blocks");
        let mut config = load_json_config("config.json").context("Could not get config file").unwrap();
        let network_config = config.networks.get_mut(&network_key).unwrap();
        network_config.num_blocks = num_blocks;
        upload_json_config("config.json", config)?;

        Ok(blocks_path) // Use the filename obtained from the redirect 
    }

    async fn get_and_download_transactions(
        &self,
        network: &NetworkType,
        highest_slot: u64,
        num_blocks: u64
    ) -> anyhow::Result<(PathBuf, usize)> {
        info!("Fetching transactions...");
        
        let network_key = network.to_string();
        let cache_dir = PathBuf::from("./cache");
        let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
        fs::exists(&snapshot_dir).context("Failed to get snapshot directory")?;

        // Calculate final path using the actual filename
        let txs_path = snapshot_dir.join("transactions.json");

        // Check if the final destination already exists
        if txs_path.exists() {
            info!("Found existing transactions file. Removing it to download new set of blocks.");
            fs::remove_file(&txs_path).context("Failed to delete existing block file")?;
       } 
        
        info!("Creating new transactions json file at {:?}", txs_path);
        let mut txs_file = fs::File::create(&txs_path).context("Failed to create transactions file")?;    
        
        let block_ids = self.get_finalized_slots_between(highest_slot - num_blocks, highest_slot).await?;
        let mut transactions = vec![];
        for id in block_ids {
            let block = self.get_block(id).await.unwrap();
            for tx in block.transactions.unwrap() {
                if !tx.meta.as_ref().unwrap().err.is_some() {
                    transactions.push(tx.transaction);
                }
            }
        }
        
        let json_content = serde_json::to_vec(&transactions).unwrap();
        txs_file.write_all(&json_content).context("Failed to write transactions to file")?;
        
        info!("Updating config file with num of blocks");
        let mut config = load_json_config("config.json").context("Could not get config file").unwrap();
        let network_config = config.networks.get_mut(&network_key).unwrap();
        network_config.num_blocks = num_blocks;
        network_config.num_txs = transactions.len() as u64;
        upload_json_config("config.json", config)?;

        Ok((txs_path, transactions.len()))
    }

    /// this function extracts all transactions from existing blocks json file
    async fn extract_transactions(
        &self,
        network: &NetworkType, 
        blocks_path: &PathBuf
    ) -> anyhow::Result<(PathBuf, usize)> {
        let file = fs::File::open(blocks_path).context(format!("Failed to open config file {:?}", blocks_path))?;
        let reader = io::BufReader::new(file);
        let blocks: Vec<RpcBlock> = serde_json::from_reader(reader).context("Failed to parse config file")?;
        
        let network_key = network.to_string();
        let cache_dir = PathBuf::from("./cache");
        let snapshot_dir = cache_dir.join(format!("snapshots-{}", network_key));
        fs::exists(&snapshot_dir).context("Failed to get snapshot directory")?;

        // Calculate final path using the actual filename
        let txs_path = snapshot_dir.join("transactions.json");

        // Check if the final destination already exists
        if txs_path.exists() {
            info!("Found exists blocks file. Removing it to download new set of blocks.");
            fs::remove_file(&txs_path).context("Failed to delete existing block file")?;
       } 
        
        info!("Creating new blocks json file");
        let mut txs_file = fs::File::create(&txs_path).context("Failed to create txs file")?;

        let mut transactions = vec![];
        for block in blocks {
            for tx in block.transactions.unwrap() {
               if !tx.meta.as_ref().unwrap().err.is_some() {
                    transactions.push(tx.transaction);
                }
            }
        }
        
        let json_content = serde_json::to_vec(&transactions).unwrap();
        txs_file.write_all(&json_content).context("Failed to write transactions to file")?;

        info!("Updating config file with num of blocks");
        let mut config = load_json_config("config.json").context("Could not get config file").unwrap();
        let network_config = config.networks.get_mut(&network_key).unwrap();
        network_config.num_txs = transactions.len() as u64;
        upload_json_config("config.json", config)?;
        
        Ok((txs_path, transactions.len()))
    }
}
