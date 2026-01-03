//! An implementation of the Redis Serialization Protocol.

use std::io;

use anyhow::anyhow;

/// Represents a Redis object.
#[derive(Debug, PartialEq)]
pub enum Object {
    /// An array of objects.
    Array(Vec<Box<Object>>),

    /// A bulk string object. Bulk strings may have `\r` or `\n`.
    BulkString(Vec<u8>),
}

impl Object {
    pub fn serialize<T: io::Write>(&self, stream: &mut T) -> io::Result<()> {
        match self {
            Object::Array(elements) => {
                write!(stream, "*{}\r\n", elements.len())?;
                for e in elements.iter() {
                    e.serialize(stream)?;
                }
                Ok(())
            }
            Object::BulkString(string) => {
                write!(stream, "${}\r\n", string.len())?;
                stream.write(&string)?;
                write!(stream, "\r\n")
            }
        }
    }
}

/// Deserializes a Redis object.
pub fn deserialize_object<T: io::Read>(
    state: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<Object> {
    match state.current(stream)? {
        Some(b'$') => deserialize_bulk_string(state, stream),
        Some(b'*') => deserialize_array(state, stream),
        Some(b) => Err(anyhow!("byte is not a data type: {:x}", b)),
        None => Err(anyhow!("unexpected end of state")),
    }
}

/// Deserializes a Redis array.
fn deserialize_array<T: io::Read>(input: &mut ReadState, stream: &mut T) -> anyhow::Result<Object> {
    assert_eq!(input.current(stream)?, Some(b'*'));
    input.advance();
    let length = read_digits(input, stream)?;
    let length = parse_u32(&length)?;
    expect_delimiter(input, stream)?;
    let mut elements = Vec::new();
    for _ in 0..length {
        let element = deserialize_object(input, stream)?;
        elements.push(Box::new(element));
    }
    Ok(Object::Array(elements))
}

/// Deserializes a Redis bulk string.
fn deserialize_bulk_string<T: io::Read>(
    input: &mut ReadState,
    stream: &mut T,
) -> anyhow::Result<Object> {
    assert_eq!(input.current(stream)?, Some(b'$'));
    input.advance();
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
    Ok(Object::BulkString(string))
}

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

/// Returns whether a byte is an ASCII digit.
fn is_digit(b: u8) -> bool {
    b'0' <= b && b <= b'9'
}

fn _display_byte(b: u8) {
    match b {
        0x0A => print!("LF"),
        0x0D => print!("CR"),
        0x20 => print!("SP"),
        b if b < 0x20 => print!("{:x}", b),
        b => print!("{}", b as char),
    }
}

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

    pub fn advance(&mut self) {
        self.offset += 1;
    }

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

    fn read_more<T: io::Read>(&mut self, stream: &mut T) -> io::Result<()> {
        self.length = stream.read(&mut self.buffer)?;
        self.can_read_more = self.length > 0;
        self.offset = 0;
        Ok(())
    }
}
