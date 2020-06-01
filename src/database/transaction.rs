use std::fs;
use std::fs::File;
use std::cmp;
use std::path::Path;
use std::error::Error;
use std::collections::HashMap;
use crate::wal::{WalReference, WalOffset};

type Result<T>     = std::result::Result<T, Box<dyn Error>>;
type TransactionId = u64;
type FileId        = u64;
type PageId        = u64;

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
        Err("not implemented".into())
    }

    // In the context of `txn`, read a page from `file` at `offset`. Note that
    // the offset is a page offset, not a memory offset. Client code is
    // advised to cache reads until it is necessary to commit them.
    pub fn read_page(&mut self, txn: TransactionId, file: FileId, offset: PageId) -> Result<[u8; 4096]> {
        Err("not implemented".into())
    }

    // In the context of `txn`, write a page to `file` at `offset`. Note that
    // the offset is a page offset, not a memory offset. Client code is
    // advised to cache writes until it is necessary to commit them.
    pub fn write_page(&mut self, txn: TransactionId, file: FileId, offset: PageId, contents: &[u8; 4096]) -> Result<()> {
        Err("not implemented".into())
    }

    pub fn wal(&mut self) -> &mut WalReference {
        &mut self.wal
    }
}
