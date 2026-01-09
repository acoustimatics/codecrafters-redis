//! Engine to implement a Redis-like data store.

use std::collections::{HashMap, VecDeque};

/// All the possible kind types of objects the engine deals with.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Object {
    /// An array of objects.
    Array(Vec<Box<Object>>),

    /// A bulk string object. Bulk strings may have `\r` or `\n`.
    BulkString(Option<Vec<u8>>),

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

/// Holds the current state of the engine.
pub struct Engine {
    /// The key/value data store.
    data: HashMap<Object, Object>,
}

impl Engine {
    pub fn new() -> Self {
        let data = HashMap::new();
        Self { data }
    }

    /// Do the command described in the given object.
    pub fn do_command(&mut self, object: Object) -> Object {
        let Object::Array(elements) = object else {
            return Object::new_error(b"expected an non-empty array");
        };

        let mut elements = VecDeque::from(elements);

        let Some(command) = elements.pop_front() else {
            return Object::new_error(b"expected an non-empty array");
        };

        let Object::BulkString(Some(mut command)) = *command else {
            return Object::new_error(b"expected first element to be a non-null bulk string");
        };

        for i in 0..command.len() {
            command[i] = command[i].to_ascii_uppercase();
        }

        match command.as_slice() {
            b"GET" => self.do_get(elements),
            b"PING" => Object::new_simple_string(b"PONG"),
            b"ECHO" => self.do_echo(elements),
            b"SET" => self.do_set(elements),
            _ => Object::new_error(b"unknown command"),
        }
    }

    /// Do an echo command. This returns the arguments as is back to the client.
    fn do_echo(&mut self, mut elements: VecDeque<Box<Object>>) -> Object {
        let Some(arg) = elements.pop_front() else {
            return Object::new_error(b"ECHO requires an argument");
        };

        if !elements.is_empty() {
            return Object::new_error(b"ECHO requires exactly one argument");
        }

        *arg
    }

    /// Do an set command.
    fn do_set(&mut self, mut elements: VecDeque<Box<Object>>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"SET requires a key argument");
        };

        let Some(value) = elements.pop_front() else {
            return Object::new_error(b"SET requires a value argument");
        };

        if !elements.is_empty() {
            return Object::new_error(b"SET requires exactly two arguments");
        }

        let _ = self.data.insert(*key, *value);

        Object::new_simple_string(b"OK")
    }

    /// Do a get command.
    fn do_get(&mut self, mut elements: VecDeque<Box<Object>>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"GET requires a key argument");
        };

        if !elements.is_empty() {
            return Object::new_error(b"GET requires exactly one argument");
        }

        if let Some(value) = self.data.get(&key) {
            value.clone()
        } else {
            Object::BulkString(None)
        }
    }
}
