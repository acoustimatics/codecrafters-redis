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

    Integer(i64),

    /// A simple string object. May not have `\r\n`.
    SimpleString(Vec<u8>), // TODO: Confirm somehow this doesn't have `\r\n`?
}

impl Object {
    /// Create a new array object from a vector.
    pub fn new_array(items: Vec<Object>) -> Self {
        let array = ObjectArray { items };
        Object::Array(array)
    }

    /// Create a new empty array.
    pub fn new_empty_array() -> Self {
        Self::new_array(Vec::new())
    }

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

impl ObjectArray {
    fn lrange(&self, start: usize, stop: usize) -> &[Object] {
        let len = self.items.len();
        if start >= len || start > stop {
            return &self.items[0..0];
        }
        let stop = std::cmp::min(stop, len - 1) as usize;
        &self.items[start..=stop]
    }
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
            b"RPUSH" => self.do_rpush(elements),
            b"SET" => self.do_set(elements),
            b"LRANGE" => self.do_lrange(elements),
            _ => Object::new_error(b"unknown command"),
        }
    }

    fn do_lrange(&mut self, mut elements: VecDeque<Object>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"LRANGE requires a key argument");
        };

        let Some(Object::BulkString(Some(start))) = elements.pop_front() else {
            return Object::new_error(b"LRANGE requires a start index");
        };

        let Some(Object::BulkString(Some(stop))) = elements.pop_front() else {
            return Object::new_error(b"LRANGE requires a stop index");
        };

        let Some(start) = parse_usize(&start) else {
            return Object::new_error(b"couldn't parse start as an integer");
        };

        let Some(stop) = parse_usize(&stop) else {
            return Object::new_error(b"couldn't parse stop as an integer");
        };

        let Some(entry) = self.data.get(&key) else {
            return Object::new_empty_array();
        };

        // TODO: Check for expiration?

        let Object::Array(array) = &entry.value else {
            return Object::new_empty_array();
        };

        let array = array.lrange(start, stop).iter().map(|obj| obj.clone()).collect();

        Object::new_array(array)
    }

    fn do_rpush(&mut self, mut elements: VecDeque<Object>) -> Object {
        let Some(key) = elements.pop_front() else {
            return Object::new_error(b"RPUSH requires a key argument");
        };

        if elements.is_empty() {
            return Object::new_error(b"RPUSH requires an element argument");
        }

        let entry = self.data.entry(key)
            .or_insert(EntryBuilder::new(Object::new_empty_array()).build());

        let Object::Array(array) = &mut entry.value  else {
            return Object::new_error(b"object at key is not an array");
        };

        while let Some(element) = elements.pop_front() {
            array.items.push(element);
        }

        Object::Integer(array.items.len() as i64)
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

fn parse_usize(s: &[u8]) -> Option<usize> {
    let mut n = 0;
    for b in s.iter() {
        if b'0' <= *b && *b <= b'9' {
            let d = (b - b'0') as usize;
            n = 10 * n + d;
        } else {
            return None;
        }
    }
    Some(n)
}
