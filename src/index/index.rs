use crate::{query::Constant, record::RID, DbResult};

pub trait Index {
    /// Position the index before the first record having the specified search key
    fn before_first(&mut self, search_key: &Constant) -> DbResult<()>;

    /// Move to the next record having the search key specified in before_first
    /// Returns false if there are no more index records with that search key
    fn next(&mut self) -> DbResult<bool>;

    /// Get the RID stored in the current index record
    fn get_data_rid(&self) -> DbResult<RID>;

    /// Insert an index record with the specified value and RID
    fn insert(&mut self, data_val: &Constant, data_rid: &RID) -> DbResult<()>;

    /// Delete the index record with the specified value and RID
    fn delete(&mut self, data_val: &Constant, data_rid: &RID) -> DbResult<()>;

    fn close(&mut self);
}