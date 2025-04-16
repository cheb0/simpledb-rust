use storage::block_id::BlockId;
use tx::recovery::{log_record::{create_log_record, LogRecord, SETINT_FLAG, SETSTRING_FLAG, START_FLAG, COMMIT_FLAG, ROLLBACK_FLAG, CHECKPOINT_FLAG}, set_int_record::SetIntRecord};

mod storage;
mod log;
mod buffer;
mod error;
mod tx;

fn main() {
    
}
