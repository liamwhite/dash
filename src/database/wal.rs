use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub type WalOffset = u64;

#[repr(C)]
pub enum WalEvent {
    ModifyPage,
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

    // Log that a page is to be modified.
    pub fn modify_page(&self, transaction_id: u64, file_id: u64, page_id: u64, old: &[u8; 4096], new: &[u8; 4096]) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // Log that a transaction is to be committed.
    pub fn commit_transaction(&self, transaction_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // Log that a file is to be extended by `extend_pages` pages.
    pub fn extend_file(&self, transaction_id: u64, file_id: u64, extend_pages: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // Log that a file is to be shrunk by `shrink_pages` pages.
    pub fn shrink_file(&self, transaction_id: u64, file_id: u64, shrink_pages: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // Log that an empty file with ID `file_id` is to be created.
    pub fn create_file(&self, transaction_id: u64, file_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // Read an entry at a given offset from the log.
    pub fn read_entry(&self, offset: WalOffset) -> Result<WalEntry> {
        Err("not implemented".into())
    }

    // Truncate (remove) the WAL. To be done during checkpointing and crash
    // recovery.
    pub fn truncate(&self) -> Result<()> {
        Err("not implemented".into())
    }
}
