use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub type WalOffset = u64;

#[repr(C)]
pub enum WalEvent {
    ModifyPage,
    BeginTransaction,
    CommitTransaction,
    ExtendFile,
    ShrinkFile,
    CreateFile
}

#[repr(C)]
pub struct WalEntry {
    event:          WalEvent,
    transaction_id: u64,
    file_id:        u64,
    page_id:        u64,
    undo:           [u8; 4096],
    redo:           [u8; 4096]
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

    pub fn modify_page(&self, transaction_id: u64, file_id: u64, page_id: u64, old: &[u8; 4096], new: &[u8; 4096]) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn begin_transaction(&self, transaction_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn commit_transaction(&self, transaction_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn extend_file(&self, transaction_id: u64, file_id: u64, extend_amount: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn shrink_file(&self, transaction_id: u64, file_id: u64, shrink_amount: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // possibly need to rethink this
    pub fn create_file(&self, transaction_id: u64, file_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn read_entry(&self, offset: WalOffset) -> Result<WalEntry> {
        Err("not implemented".into())
    }
}