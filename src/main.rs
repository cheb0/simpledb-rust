use storage::block_id::BlockId;
use tx::recovery::{log_record::{create_log_record, LogRecord, SETINT_FLAG, SETSTRING_FLAG, START_FLAG, COMMIT_FLAG, ROLLBACK_FLAG, CHECKPOINT_FLAG}, set_int_record::SetIntRecord};

mod storage;
mod log;
mod buffer;
mod error;
mod tx;

fn main() {
    let blk = BlockId::new("asfjdsf".to_string(), 64544);
    let record = SetIntRecord::create(6, blk, 7, 2);
    let bytes = record.to_bytes().unwrap();
    println!("res len: {}", bytes.len());

    let deser = create_log_record(bytes.as_slice()).unwrap();
    
    match deser.op() {
        CHECKPOINT_FLAG => println!("Checkpoint record"),
        START_FLAG => println!("Start record for transaction {}", deser.tx_number()),
        COMMIT_FLAG => println!("Commit record for transaction {}", deser.tx_number()),
        ROLLBACK_FLAG => println!("Rollback record for transaction {}", deser.tx_number()),
        SETINT_FLAG => {
            println!("SetInt record for transaction {}", deser.tx_number());
            // We can downcast to get the specific record type if needed
            if let Some(set_int) = (&*deser).as_any().downcast_ref::<SetIntRecord>() {
                println!("  Block: {}, Offset: {}, Value: {}", 
                         set_int.blk.filename(), set_int.offset, set_int.val);
            }
        },
        SETSTRING_FLAG => println!("SetString record for transaction {}", deser.tx_number()),
        _ => println!("Unknown record type: {}", deser.op()),
    }
}
