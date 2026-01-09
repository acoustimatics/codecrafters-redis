//! Engine to implement a Redis-like data store.

use std::collections::VecDeque;

/// All the possible kind types of objects the engine deals with.
#[derive(Debug, Eq, Hash, PartialEq)]
pub enum Object {
    /// An array of objects.
    Array(Vec<Box<Object>>),

    /// A bulk string object. Bulk strings may have `\r` or `\n`.
    BulkString(Vec<u8>),

    /// An error with a message.
    Error(Vec<u8>), // TODO: Confirm somehow this doesn't have `\r\n`?

    /// A simple string object. May not have `\r\n`.
    SimpleString(Vec<u8>), // TODO: Confirm somehow this doesn't have `\r\n`?
}

impl Object {
    /// Create a new error object from a byte slice.
    pub fn new_error(message: &[u8]) -> Self {
        let message = Vec::from(message);
        Object::Error(message)
    }

    /// Create a simple string from a byte slice.
    pub fn new_simple_string(string: &[u8]) -> Self {
        let string = Vec::from(string);
        Object::SimpleString(string)
    }
}

/// Do the command described in the given object.
pub fn do_command(object: Object) -> Object {
    let Object::Array(elements) = object else {
        return Object::new_error(b"expected an non-empty array");
    };

    let mut elements = VecDeque::from(elements);

    let Some(command) = elements.pop_front() else {
        return Object::new_error(b"expected an non-empty array");
    };

    let Object::BulkString(mut command) = *command else {
        return Object::new_error(b"expected first element to be a bulk string");
    };

    for i in 0..command.len() {
        command[i] = command[i].to_ascii_uppercase();
    }

    match command.as_slice() {
        b"PING" => Object::new_simple_string(b"PONG"),
        b"ECHO" => do_echo(elements),
        _ => Object::new_error(b"unknown command"),
    }
}

/// Do an echo command. This returns the arguments as is back to the client.
fn do_echo(mut elements: VecDeque<Box<Object>>) -> Object {
    let Some(arg) = elements.pop_front() else {
        return Object::new_error(b"ECHO requires an argument");
    };

    if !elements.is_empty() {
        return Object::new_error(b"ECHO requires exactly one argument");
    }

    *arg
}
