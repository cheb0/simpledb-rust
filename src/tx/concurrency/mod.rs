pub mod concurrency_mgr;
pub mod lock_table;

pub use concurrency_mgr::ConcurrencyMgr;
pub use lock_table::LockTable;

#[derive(Debug, Clone, PartialEq)]
pub enum LockType {
    Shared,
    Exclusive,
}