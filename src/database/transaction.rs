use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::SeekFrom;
use std::io::prelude::*;
use std::cmp;
use std::path::Path;
use std::error::Error;
use std::collections::HashMap;
use crate::database::wal::*;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub type TransactionId = u64;
pub type FileId        = u64;
pub type PageId        = u64;

#[derive(Eq, PartialEq, Hash)]
pub struct PageOffset {
    file_id: FileId,
    page_id: PageId
}

pub struct TransactionWalOffset {
    transaction_id: TransactionId,
    wal_offset:     WalOffset
}

pub struct TransactionManager {
    // WAL reference used to guarantee atomicity of changes.
    //
    // Write-ahead logging is the typical technique used for ensuring changes
    // made to a database are durable and atomic.
    wal:          WalReference,

    // Base path which is used to construct and open new data files. Data files
    // are referred to by index using a u64.
    base_path:    String,

    // Cached mapping of file descriptors. If a file requested is not in the
    // cache, it will be opened and then cached in the map.
    files:        HashMap<FileId, File>,

    // Cached mapping of modified pages. Used to accelerate reads. Cleared
    // during checkpointing.
    page_events:  HashMap<PageOffset, Vec<TransactionWalOffset>>,

    // Cached mapping of modified files. Used to accelerate reads. Cleared
    // during checkpointing.
    file_events:  HashMap<FileId, Vec<TransactionWalOffset>>,

    // Mapping of active transactions to WAL entries they have made. Used by
    // the checkpointer to determine how much progress can be made, and by
    // the transaction manager to implement rollback.
    transactions: HashMap<TransactionId, Vec<WalOffset>>,

    // ID of the most recent transaction. Used only for issuing new IDs.
    // Transaction IDs are ephemeral and reset from zero every time the
    // database server is restarted.
    transaction:  u64,

    // ID of the most recently created file. Used only for issuing new IDs.
    // File IDs do not reset after deletion.
    file:         u64
}

impl TransactionManager {
    pub fn new(base_path: &String) -> Result<TransactionManager> {
        // Find most recent file ID. Mutable state used because
        // read_dir iterator can fail.
        let mut file = 0;

        for entry in fs::read_dir(Path::new(base_path))? {
            match entry?.file_name().into_string() {
                Ok(entry) => {
                    match str::parse::<u64>(&entry) {
                        Ok(val) => { file = cmp::max(file, val) },
                        Err(_) => {}
                    }
                }
                Err(_) => {}
            }
        }

        Ok(TransactionManager {
            wal:          WalReference::new(&format!("{}/WAL", base_path))?,
            base_path:    base_path.clone(),
            files:        HashMap::new(),
            page_events:  HashMap::new(),
            file_events:  HashMap::new(),
            transactions: HashMap::new(),
            transaction:  0,
            file:         file
        })
    }

    // Begin a transaction. This returns a new transaction ID which is used
    // as a handle for a client to access the database.
    pub fn begin_transaction(&mut self) -> Result<TransactionId> {
        self.transaction += 1;
        self.transactions.insert(self.transaction, vec![]);

        Ok(self.transaction)
    }

    // In the context of `txn`, read a page from `file` at `offset`. Note that
    // the offset is a page offset, not a memory offset. Client code is
    // advised to cache reads until it is necessary to commit them.
    pub fn read_page(&mut self, txn: TransactionId, file: FileId, offset: PageId) -> Result<[u8; 4096]> {
        // Check if this file is visible to the current transaction:
        if !self.file_exists_for_transaction(txn, file)? {
            return Err("invalid file".into())
        }

        // Check if this page is visible to the current transaction:
        if !self.page_exists_for_transaction(txn, file, offset)? {
            return Err("invalid read address".into())
        }

        // File exists and page offset is visible, check what to return:
        match self.get_page_from_undo_cache(txn, file, offset) {
            None => self.get_page_from_file(file, offset),
            Some(page) => Ok(page)
        }
    }

    // In the context of `txn`, write a page to `file` at `offset`. Note that
    // the offset is a page offset, not a memory offset. Client code is
    // advised to cache writes until it is necessary to commit them.
    pub fn write_page(&mut self, txn: TransactionId, file: FileId, offset: PageId, new_page: &[u8; 4096]) -> Result<()> {
        let old_page = self.read_page(txn, file, offset)?;
        let wal_offset = self.wal.modify_page(txn, file, offset, &old_page, new_page)?;

        self.add_page_event(txn, file, offset, wal_offset);
        self.add_event_to_transaction_list(txn, wal_offset);

        Ok(())
    }

    pub fn wal(&mut self) -> &mut WalReference {
        &mut self.wal
    }

    // Check whether the file `file` can be read by the given transaction.
    fn file_exists_for_transaction(&mut self, txn: TransactionId, file: FileId) -> Result<bool> {
        // Likely: no delete file events have occurred
        if let Some(list) = self.file_events.get(&file) {
            return Err("not implemented".into())
        }

        // Likely: the file is already open
        if let Some(file_ref) = self.files.get(&file) {
            return Ok(true)
        }

        // Likely: the file exists on disk
        let path = format!("{}/{}", self.base_path, file);
        Ok(Path::new(&path).exists())
    }

    // Check whether the page `page` can be read by the given transaction.
    fn page_exists_for_transaction(&mut self, txn: TransactionId, file: FileId, offset: PageId) -> Result<bool> {
        Ok(false)
    }

    fn get_page_from_undo_cache(&mut self, txn: TransactionId, file: FileId, offset: PageId) -> Option<[u8; 4096]> {
        // Two cases:
        None
    }

    fn get_page_from_file(&mut self, file: FileId, offset: PageId) -> Result<[u8; 4096]> {
        // Get open file or cached reference
        let file_ref = match self.files.get_mut(&file) {
            Some(file_ref) => file_ref,
            None => {
                let path = format!("{}/{}", self.base_path, file);
                let file_ref = OpenOptions::new().read(true).write(true).open(Path::new(&path))?;
                self.files.insert(file, file_ref);
                self.files.get_mut(&file).unwrap()
            }
        };

        file_ref.seek(SeekFrom::Start(offset * 4096))?;

        let mut buf = [0u8; 4096];
        assert_eq!(file_ref.read(&mut buf)?, 4096);

        Ok(buf)
    }

    fn get_undo_page_from_wal_event(&mut self, wal_offset: WalOffset) -> Result<[u8; 4096]> {
        let entry = self.wal.read_entry(wal_offset)?;

        match entry {
            WalEvent::ModifyPageEvent { transaction_id: _, file_id: _, page_id: _, undo, redo: _ } => Ok(undo),
            _ => Err("invalid WAL offset".into())
        }
    }

    fn get_redo_page_from_wal_event(&mut self, wal_offset: WalOffset) -> Result<[u8; 4096]> {
        let entry = self.wal.read_entry(wal_offset)?;

        match entry {
            WalEvent::ModifyPageEvent { transaction_id: _, file_id: _, page_id: _, undo: _, redo } => Ok(redo),
            _ => Err("invalid WAL offset".into())
        }
    }

    fn add_page_event(&mut self, txn: TransactionId, file: FileId, offset: PageId, wal_offset: WalOffset) {
        self.page_events
            .entry(PageOffset { file_id: file, page_id: offset })
            .or_insert_with(|| vec![])
            .push(TransactionWalOffset { wal_offset, transaction_id: txn });
    }

    fn add_event_to_transaction_list(&mut self, txn: TransactionId, wal_offset: WalOffset) {
        self.transactions
            .get_mut(&txn)
            .unwrap()
            .push(wal_offset);
    }
}
