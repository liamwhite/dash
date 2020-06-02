use std::ops::Drop;
use std::error::Error;
use std::collections::{HashSet, HashMap};
use crate::database::transaction::*;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub struct MemoryProxy<'a> {
    working_memory: HashMap<PageId, [u8; 4096]>,
    modified_set:   HashSet<PageId>,
    transaction:    TransactionId,
    file:           FileId,
    manager:        &'a mut TransactionManager
}

impl<'a> MemoryProxy<'a> {
    pub fn new(txn: TransactionId, file: FileId, manager: &'a mut TransactionManager) -> MemoryProxy<'a> {
        MemoryProxy {
            working_memory: HashMap::new(),
            modified_set:   HashSet::new(),
            transaction:    txn,
            file:           file,
            manager:        manager
        }
    }

    // Read a page-aligned value of size `size` at `address`.
    fn read_aligned(&mut self, address: usize, size: usize) -> Result<Vec<u8>> {
        let page_addr   = (address & 0xfffusize) as u64;
        let page_offset = address & !0xfffusize;

        // Ideally we'd use entry, but the closure makes returning an error type annoying.
        let page = match self.working_memory.get(&page_addr) {
            Some(p) => p,
            None => {
                let page = self.manager.read_page(self.transaction, self.file, page_addr)?;
                self.working_memory.insert(page_addr, page);
                self.working_memory.get(&page_addr).unwrap()
            }
        };

        Ok(page[page_offset .. page_offset + size].to_vec())
    }
}
