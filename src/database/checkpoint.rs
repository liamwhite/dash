use std::error::Error;
use crate::database::wal::*;
use crate::database::transaction::TransactionManager;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

// Apply a WAL entry to the filesystem. To be done during checkpointing
// and crash recovery.
pub fn apply_wal_event(event: &WalEvent, manager: &mut TransactionManager) -> Result<()> {
    //match event {
    //    ModifyPageEvent => {},
    //    CommitTransactionEvent => {},
    //    ExtendFileEvent => {},
    //    ShrinkFileEvent => {},
    //    CreateFileEvent => {}
    //}

    Err("not implemented".into())
}

// Try to make progress checkpointing. Will fail if no progress can
// be made yet, allowing the caller to put the thread to sleep as
// necessary.
pub fn checkpoint_step(manager: &mut TransactionManager) -> Result<()> {
    Err("not implemented".into())
}

// Read and apply all valid WAL entries. If junk occurs at the end of
// the WAL, it is ignored.
pub fn crash_recover(manager: &mut TransactionManager) -> Result<()> {
    Err("not implemented".into())
}
