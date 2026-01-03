//! Engine to implement a Redis-like data store.

#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Object {
    /// An array of objects.
    Array(Vec<Box<Object>>),

    /// A bulk string object. Bulk strings may have `\r` or `\n`.
    BulkString(Vec<u8>),

    /// A simple string object. May not have `\r\n`.
    SimpleString(Vec<u8>), // TODO: Confirm somehow this doesn't have `\r\n`?
}

impl Object {
    /// Create a simple string from a byte slice. The bytes are copied.
    pub fn new_simple_string(string: &[u8]) -> Self {
        let string = Vec::from(string);
        Object::SimpleString(string)
    }
}
