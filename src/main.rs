#![allow(unused)]

mod engine;
mod resp;

use std::collections::VecDeque;
use std::io;
use std::net;
use std::thread;

use anyhow::anyhow;

fn main() {
    let mut connection_id = 0;
    match net::TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        connection_id += 1;
                        println!("[id={connection_id}] accepted new connection");
                        thread::spawn(move || handle_connection(connection_id, stream));
                    }
                    Err(e) => {
                        eprintln!("error accepting connection {e}");
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("error binding TCP listener: {e}");
        }
    }
}

fn handle_connection<T: io::Read + io::Write>(id: usize, stream: T) {
    match read_respond_loop(id, stream) {
        Ok(_) => println!("[id={id}] closed connection"),
        Err(e) => eprintln!("{e}"),
    }
}

fn read_respond_loop<T: io::Read + io::Write>(id: usize, mut stream: T) -> anyhow::Result<()> {
    let mut read_state = resp::ReadState::new();
    while read_state.can_read_more {
        let object = resp::deserialize_object(&mut read_state, &mut stream)?;
        match do_command(object, &mut stream) {
            Ok(()) => (),
            Err(e) => {
                eprintln!("[id={id}] error: {e}");
                write!(stream, "-{e}\r\n");
            },
        }
    }
    Ok(())
}

fn do_command<T: io::Write>(object: engine::Object, stream: &mut T) -> anyhow::Result<()> {
    let engine::Object::Array(elements) = object else {
        return Err(anyhow!("expected an non-empty array"));
    };

    let mut elements = VecDeque::from(elements);

    let Some(command) = elements.pop_front() else {
      return Err(anyhow!("expected an non-empty array"));
    };

    let engine::Object::BulkString(mut command) = *command else {
        return Err(anyhow!("expected first element to be a bulk string"));
    };

    for i in 0..command.len() {
        command[i] = command[i].to_ascii_uppercase();
    }

    match command.as_slice() {
        b"PING" => {
            let pong = engine::Object::new_simple_string(b"PONG");
            resp::serialize(stream, &pong)?;
            Ok(())
        }
        b"ECHO" => do_echo(&mut elements, stream),
        _ => Err(anyhow!("unknown command")),
    }
}

fn do_echo<T: io::Write>(elements: &mut VecDeque<Box<engine::Object>>, stream: &mut T) -> anyhow::Result<()> {
    let Some(arg) = elements.pop_front() else {
        return Err(anyhow!("ECHO requires an argument"));
    };

    if !elements.is_empty() {
        return Err(anyhow!("ECHO requires exactly one argument"));
    }
    
    if matches!(&*arg, engine::Object::BulkString(_)) {
        resp::serialize(stream, &*arg)?;
    } else {
        return Err(anyhow!("ECHO requires a bulk string argument"));
    }

    Ok(())
}
