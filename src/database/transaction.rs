use std::fs;
use std::fs::File;
use std::cmp;
use std::path::Path;
use std::error::Error;
use std::collections::HashMap;
use crate::wal::{WalReference, WalOffset};

type Result<T>     = std::result::Result<T, Box<dyn Error>>;
type TransactionId = u64;

pub struct PageOffset {
    file_id: u64,
    page_id: u64
}

pub struct TransactionWalOffset {
    transaction_id: u64,
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
    files:        HashMap<u64, File>,

    // Cached mapping of modified pages. Used to accelerate reads. Cleared
    // during checkpointing.
    pages:        HashMap<PageOffset, Vec<TransactionWalOffset>>,

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
            pages:        HashMap::new(),
            transactions: HashMap::new(),
            transaction:  0,
            file:         file
        })
    }
}
