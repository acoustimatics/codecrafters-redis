//! Engine to implement a Redis-like data store.

use std::collections::{HashMap, VecDeque};
use std::time;

/// All the possible kind types of objects the engine deals with.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Object {
    /// An array of objects.
    Array(ObjectArray),

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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ObjectArray {
    pub items: Vec<Object>,
}

/// An entry value in the data table.
struct Entry {
    /// The entry's value.
    value: Object,

    /// When the entry was created.
    created_at: time::Instant,

    /// For how long this entry is valid.
    duration: Option<time::Duration>,
}

struct EntryBuilder {
    value: Object,
    duration: Option<time::Duration>,
}

impl EntryBuilder {
    fn new(value: Object) -> Self {
        EntryBuilder {
            value,
            duration: None,
        }
    }

    fn build(self) -> Entry {
        Entry {
            value: self.value,
            created_at: time::Instant::now(),
            duration: self.duration,
        }
    }

    fn duration_ms(&mut self, duration_ms: u64) {
        self.duration = Some(time::Duration::from_millis(duration_ms));
    }
}

/// Holds the current state of the engine.
pub struct Engine {
    /// The key/value data store.
    data: HashMap<Object, Entry>,
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

        let mut elements = VecDeque::from(elements.items);

        let Some(command) = elements.pop_front() else {
            return Object::new_error(b"expected an non-empty array");
        };

        let Object::BulkString(Some(mut command)) = command else {
            return Object::new_error(b"expected first element to be a non-null bulk string");
        };

        convert_to_ascii_uppercase(&mut command);

        match command.as_slice() {
            b"GET" => self.do_get(elements),
            b"PING" => Object::new_simple_string(b"PONG"),
            b"ECHO" => self.do_echo(elements),
            b"SET" => self.do_set(elements),
            _ => Object::new_error(b"unknown command"),
        }
    }

    /// Do an echo command. This returns the arguments as is back to the client.
    fn do_echo(&mut self, mut elements: VecDeque<Object>) -> Object {
        let Some(arg) = elements.pop_front() else {
            return Object::new_error(b"ECHO requires an argument");
        };

        if !elements.is_empty() {
            return Object::new_error(b"ECHO requires exactly one argument");
        }

        arg
    }

    /// Do an set command.
    fn do_set(&mut self, mut elements: VecDeque<Object>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"SET requires a key argument");
        };

        let Some(value) = elements.pop_front() else {
            return Object::new_error(b"SET requires a value argument");
        };

        let mut entry_builder = EntryBuilder::new(value);

        match elements.pop_front() {
            Some(Object::BulkString(Some(mut option))) => {
                convert_to_ascii_uppercase(&mut option);
                match option.as_slice() {
                    b"PX" => {
                        match elements.pop_front() {
                            Some(Object::BulkString(Some(option))) => {
                                let mut duration_ms = 0;
                                for b in option {
                                    if b'0' <= b && b < b'9' {
                                        let digit = (b - b'0') as u64;
                                        duration_ms = 10 * duration_ms + digit;
                                        // TODO: Handle overflow? It's a rather big int.
                                    } else {
                                        return Object::new_error(b"Invalid PX duration");
                                    }
                                }
                                entry_builder.duration_ms(duration_ms);
                            }
                            _ => return Object::new_error(b"PX requires a duration"),
                        }
                    }
                    // TODO: Error handle other things here.
                    _ => (),
                }
            }
            // TODO: Error handle other things here.
            _ => (),
        }

        let entry = entry_builder.build();
        let _ = self.data.insert(key, entry);

        Object::new_simple_string(b"OK")
    }

    /// Do a get command.
    fn do_get(&mut self, mut elements: VecDeque<Object>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"GET requires a key argument");
        };

        if !elements.is_empty() {
            return Object::new_error(b"GET requires exactly one argument");
        }

        let Some(entry) = self.data.get(&key) else {
            return Object::BulkString(None);
        };

        let is_expired = match entry.duration {
            Some(duration) => entry.created_at + duration < time::Instant::now(),
            None => false,
        };

        if is_expired {
            return Object::BulkString(None);
        }

        entry.value.clone()
    }
}

/// Convert in place a byte slice to ASCII uppercase.
fn convert_to_ascii_uppercase(s: &mut [u8]) {
    for i in 0..s.len() {
        s[i] = s[i].to_ascii_uppercase();
    }
}
