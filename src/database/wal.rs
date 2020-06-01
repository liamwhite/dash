use std::fs::File;
use std::error::Error;

type Result<T> = std::result::Result<T, Box<dyn Error>>;
type WalOffset = u64;

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

pub struct WalReference<'a> {
    file: &'a mut File
}

impl<'a> WalReference<'a> {
    pub fn new() -> Result<WalReference> {
        Err("not implemented".into())
    }

    pub fn modify_page(&'a self, transaction_id: u64, file_id: u64, page_id: u64, old: &[u8; 4096], new: &[u8; 4096]) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn begin_transaction(&'a self, transaction_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn commit_transaction(&'a self, transaction_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn extend_file(&'a self, transaction_id: u64, file_id: u64, extend_amount: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn shrink_file(&'a self, transaction_id: u64, file_id: u64, shrink_amount: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    // possibly need to rethink this
    pub fn create_file(&'a self, transaction_id: u64, file_id: u64) -> Result<WalOffset> {
        Err("not implemented".into())
    }

    pub fn read_entry(&'a self, offset: WalOffset) -> Result<WalEntry> {
        Err("not implemented".into())
    }
}