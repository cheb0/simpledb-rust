use std::{sync::Arc, thread};

use error::DbResult;
use storage::block_id::BlockId;
use tx::concurrency::{concurrency_mgr::ConcurrencyMgr, lock_table::LockTable};

mod storage;
mod log;
mod buffer;
mod error;
mod tx;
mod record;
mod metadata;
mod server;
mod query;

fn run_test() -> DbResult<()> {
    let lock_table = Arc::new(LockTable::new());
    let mut cm1 = ConcurrencyMgr::new(Arc::clone(&lock_table));
    let blk = BlockId::new("testfile".to_string(), 1);
    
    cm1.lock_shared(&blk).unwrap();
    
    let lock_table_clone = Arc::clone(&lock_table);
    let blk_clone = blk.clone();
    
    let handle = thread::spawn(move || -> DbResult<()> {
        let mut cm2: ConcurrencyMgr = ConcurrencyMgr::new(lock_table_clone);
        cm2.lock_shared(&blk_clone).unwrap();
        
        // let x_lock_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        //     let x_lock_timeout = LockTable::with_timeout(100);
        //     let mut cm_timeout = ConcurrencyMgr::new(Arc::new(x_lock_timeout));
        //     cm_timeout.lock_exclusive(&blk_clone)
        // }));
        
        // assert!(x_lock_result.is_ok());
        // let x_lock_result = x_lock_result.unwrap();
        // assert!(x_lock_result.is_err());
        
        cm2.release();
        Ok(())
    });
    
    handle.join().unwrap()?;

    cm1.release();

    cm1.lock_exclusive(&blk)?;

    /* let lock_table_clone = Arc::clone(&lock_table);
    let blk_clone = blk.clone();
    
    let handle = thread::spawn(move || -> DbResult<()> {
        let mut cm3 = ConcurrencyMgr::new(lock_table_clone);
        cm3.lock_exclusive(&blk_clone)?;
        cm3.release();
        Ok(())
    }); */
    
    // return handle.join().unwrap();
    Ok(())
}

fn main() {
    let res = run_test();
    println!("result = {:?}", res);
}
