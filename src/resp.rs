//! An implementation of the Redis Serialization Protocol.

use std::io;

use anyhow::anyhow;

use crate::engine::{self, ObjectArray};

/// Serializes an object and writes it to a given stream.
pub fn serialize<T: io::Write>(stream: &mut T, object: &engine::Object) -> io::Result<()> {
    match object {
        engine::Object::Array(elements) => {
            write!(stream, "*{}\r\n", elements.items.len())?;
            for e in elements.items.iter() {
                serialize(stream, e)?;
            }
            Ok(())
        }
        engine::Object::Integer(i) => {
            write!(stream, ":{i}\r\n")
        }
        engine::Object::BulkString(Some(string)) => {
            write!(stream, "${}\r\n", string.len())?;
            stream.write(&string)?;
            write!(stream, "\r\n")
        }
        engine::Object::BulkString(None) => {
            write!(stream, "$-1\r\n")
        }
        engine::Object::Error(message) => {
            write!(stream, "-")?;
            stream.write(&message)?;
            write!(stream, "\r\n")
        }
        engine::Object::SimpleString(string) => {
            write!(stream, "+")?;
            stream.write(&string)?;
            write!(stream, "\r\n")
        }
    }
}

/// Deserializes an object read from a given stream.
pub fn deserialize_object<T: io::Read>(
    state: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<engine::Object> {
    match state.current(stream)? {
        Some(b'$') => deserialize_bulk_string(state, stream),
        Some(b'*') => deserialize_array(state, stream),
        Some(b':') => deserialize_integer(state, stream),
        Some(b) => Err(anyhow!("byte is not a data type: {:x}", b)),
        None => Err(anyhow!("unexpected end of state")),
    }
}

/// Deserializes an interger object.
fn deserialize_integer<T: io::Read>(
    input: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<engine::Object> {
    assert_eq!(input.current(stream)?, Some(b':'));
    input.advance();
    let sign = match input.current(stream)? {
        Some(c) if c == b'+' || c == b'-' => {
            input.advance();
            Some(c)
        }
        _ => None,
    };
    let value = read_digits(input, stream)?;
    expect_delimiter(input, stream)?;
    let value = parse_i64(&value)?;
    let value = match sign {
        Some(b'-') => -value,
        _ => value,
    };
    Ok(engine::Object::Integer(value))
}

/// Deserializes an array object.
fn deserialize_array<T: io::Read>(
    input: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<engine::Object> {
    assert_eq!(input.current(stream)?, Some(b'*'));
    input.advance();
    let length = read_digits(input, stream)?;
    let length = parse_u32(&length)?;
    expect_delimiter(input, stream)?;
    let mut items = Vec::new();
    for _ in 0..length {
        let element = deserialize_object(input, stream)?;
        items.push(element);
    }
    let array = ObjectArray { items };
    Ok(engine::Object::Array(array))
}

/// Deserializes a bulk string object.
fn deserialize_bulk_string<T: io::Read>(
    input: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<engine::Object> {
    assert_eq!(input.current(stream)?, Some(b'$'));
    input.advance();
    // TODO: Support null bulk strings that look like `$-1\r\n`.
    let length = read_digits(input, stream)?;
    let length = parse_u32(&length)?;
    expect_delimiter(input, stream)?;
    let mut string = Vec::new();
    for _ in 0..length {
        let Some(b) = input.current(stream)? else {
            return Err(anyhow!("unexpected end of input while reading bulk string"));
        };
        string.push(b);
        input.advance();
    }
    expect_delimiter(input, stream)?;
    Ok(engine::Object::BulkString(Some(string)))
}

/// Read from stream ASCII digits, putting them into a `String`.
fn read_digits<T: io::Read>(input: &mut ReadState, stream: &mut T) -> anyhow::Result<String> {
    let mut result = String::new();
    while let Some(b) = input.current(stream)?.filter(|b| is_digit(*b)) {
        result.push(b as char);
        input.advance();
    }
    Ok(result)
}

/// Advances the input if `\r\n` is found in the input. Otherwise, an error is
/// returned.
fn expect_delimiter<T: io::Read>(input: &mut ReadState, stream: &mut T) -> anyhow::Result<()> {
    expect(input, stream, b'\r')?;
    expect(input, stream, b'\n')
}

/// Advances the input if the current byte equals the expected byte. When the
/// bytes are unequal an error is returned.
fn expect<T: io::Read>(input: &mut ReadState, stream: &mut T, expected: u8) -> anyhow::Result<()> {
    match input.current(stream)? {
        Some(b) if b == expected => {
            input.advance();
            Ok(())
        }
        Some(b) => Err(anyhow!("expected {:x} but got {:x}", expected, b)),
        None => Err(anyhow!("expected {:x} but reached end of input", expected)),
    }
}

/// Parse string as an `u32` with a custom error result.
fn parse_u32(s: &str) -> anyhow::Result<u32> {
    match s.parse() {
        Ok(i) => Ok(i),
        Err(_) => Err(anyhow!(
            "couldn't parse `{s}` as an unsigned 32 bit integer"
        )),
    }
}

/// Parse string as an `i64` with a custom error result.
fn parse_i64(s: &str) -> anyhow::Result<i64> {
    match s.parse() {
        Ok(i) => Ok(i),
        Err(_) => Err(anyhow!(
            "couldn't parse `{s}` as a signed 64 bit integer"
        )),
    }
}

/// Returns whether a byte is an ASCII digit.
fn is_digit(b: u8) -> bool {
    b'0' <= b && b <= b'9'
}

/// Attempts to print the byte as an ASCII character.
#[allow(unused)]
pub fn display_byte(b: u8) {
    match b {
        0x0A => print!("\\n"),
        0x0D => print!("\\r"),
        b if b < 0x20 => print!("{:x}", b),
        b => print!("{}", b as char),
    }
}

/// Attempts to print a byte slice as ASCII characters.
#[allow(unused)]
pub fn display_byte_slice(bs: &[u8]) {
    for b in bs.iter() {
        display_byte(*b);
    }
}

/// Holds a bufferred read of a stream. Allows client code to go through the
/// bytes of a stream one at a time without taking ownership of the stream.
/// The only other way to iterate over bytes in a stream took complete
/// ownership which made it so you could never write a response to the stream.
pub struct ReadState {
    /// Bytes that have been read.
    buffer: [u8; 1024],

    /// How many bytes were read into the buffer.
    length: usize,

    /// Offset in buffer of the current byte.
    offset: usize,

    pub can_read_more: bool,
}

impl ReadState {
    /// Create a new `ReadState` and initialize it with some read bytes.
    pub fn new() -> Self {
        ReadState {
            buffer: [0; 1024],
            length: 0,
            offset: 0,
            can_read_more: true,
        }
    }

    /// Advance where the current byte is in the buffer.
    pub fn advance(&mut self) {
        self.offset += 1;
    }

    /// Get the current char in the buffer, if there is one. This may read
    /// from the stream if the buffer is empty or has all ben read. If a read
    /// returned zero bytes, then `can_read_more` will be set to false, no
    /// reads will happen, and current will return None.
    pub fn current<T: io::Read>(&mut self, stream: &mut T) -> io::Result<Option<u8>> {
        if self.offset < self.length {
            Ok(Some(self.buffer[self.offset]))
        } else if self.can_read_more {
            self.read_more(stream)?;
            self.current(stream)
        } else {
            Ok(None)
        }
    }

    /// Helper method to read from the stream into the buffer.
    fn read_more<T: io::Read>(&mut self, stream: &mut T) -> io::Result<()> {
        self.length = stream.read(&mut self.buffer)?;
        self.can_read_more = self.length > 0;
        self.offset = 0;

        print!("read {} bytes: ", self.length);
        for i in 0..self.length {
            display_byte(self.buffer[i]);
        }
        println!();

        Ok(())
    }
}
