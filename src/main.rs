mod storage;

fn main() {
    println!("Hello, world!");
    
    // Test the BlockId struct
    let block_id = storage::block_id::BlockId::new("testfile".to_string(), 3);
    println!("Block ID: {} #{}", block_id.filename(), block_id.number());
}
