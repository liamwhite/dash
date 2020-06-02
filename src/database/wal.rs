use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::path::Path;
use std::error::Error;
use serde::{Serialize, Deserialize};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub type WalOffset = u64;

big_array! { BigArray; 4096 }

#[derive(Serialize, Deserialize)]
pub enum WalEvent {
    ModifyPageEvent {
        transaction_id: u64,
        file_id:        u64,
        page_id:        u64,

        #[serde(with = "BigArray")]
        undo:           [u8; 4096],

        #[serde(with = "BigArray")]
        redo:           [u8; 4096]
    },
    CommitTransactionEvent {
        transaction_id: u64
    },
    ExtendFileEvent {
        transaction_id: u64,
        file_id:        u64,
        extend_amt:     i64,
    },
    CreateFileEvent {
        transaction_id: u64,
        file_id:        u64
    },
    DeleteFileEvent {
        transaction_id: u64,
        file_id:        u64
    }
}

pub struct WalReference {
    file: File
}

impl WalReference {
    pub fn new(path: &String) -> Result<WalReference> {
        let path = Path::new(path);

        if !path.exists() {
            File::create(path)?;
        }

        Ok(WalReference { file: OpenOptions::new().read(true).append(true).open(path)? })
    }

    // Log that a page is to be modified.
    pub fn modify_page(&mut self, transaction_id: u64, file_id: u64, page_id: u64, undo: &[u8; 4096], redo: &[u8; 4096]) -> Result<WalOffset> {
        let event = WalEvent::ModifyPageEvent { transaction_id, file_id, page_id, undo: undo.clone(), redo: redo.clone() };
        let position = self.file.seek(SeekFrom::End(0))?;

        bincode::serialize_into(&self.file, &event)?;
        self.file.sync_data()?;

        Ok(position)
    }

    // Log that a transaction is to be committed.
    pub fn commit_transaction(&mut self, transaction_id: u64) -> Result<WalOffset> {
        let event = WalEvent::CommitTransactionEvent { transaction_id };
        let position = self.file.seek(SeekFrom::End(0))?;

        bincode::serialize_into(&self.file, &event)?;
        self.file.sync_data()?;

        Ok(position)
    }

    // Log that a file is to be extended by `extend_pages` pages.
    pub fn extend_file(&mut self, transaction_id: u64, file_id: u64, extend_amt: i64) -> Result<WalOffset> {
        let event = WalEvent::ExtendFileEvent { transaction_id, file_id, extend_amt };
        let position = self.file.seek(SeekFrom::End(0))?;

        bincode::serialize_into(&self.file, &event)?;
        self.file.sync_data()?;

        Ok(position)
    }

    // Log that an empty file with ID `file_id` is to be created.
    pub fn create_file(&mut self, transaction_id: u64, file_id: u64) -> Result<WalOffset> {
        let event = WalEvent::CreateFileEvent { transaction_id, file_id };
        let position = self.file.seek(SeekFrom::End(0))?;

        bincode::serialize_into(&self.file, &event)?;
        self.file.sync_data()?;

        Ok(position)
    }

    // Log that a file with ID `file_id` is to be deleted.
    pub fn delete_file(&mut self, transaction_id: u64, file_id: u64) -> Result<WalOffset> {
        let event = WalEvent::DeleteFileEvent { transaction_id, file_id };
        let position = self.file.seek(SeekFrom::End(0))?;

        bincode::serialize_into(&self.file, &event)?;
        self.file.sync_data()?;

        Ok(position)
    }

    // Read an entry at a given offset from the log.
    pub fn read_entry(&mut self, offset: WalOffset) -> Result<WalEvent> {
        let position = self.file.seek(SeekFrom::Start(offset))?;

        Ok(bincode::deserialize_from::<_, WalEvent>(&self.file)?)
    }

    // Truncate (remove) the WAL. To be done during checkpointing and crash
    // recovery.
    pub fn truncate(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        self.file.set_len(0)?;
        Ok(self.file.sync_data()?)
    }
}
