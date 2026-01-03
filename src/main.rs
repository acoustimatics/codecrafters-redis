mod resp;

use std::io;
use std::net;
use std::thread;

use anyhow::anyhow;

fn main() {
    match net::TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        println!("accepted new connection");
                        thread::spawn(move || handle_connection(stream));
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

fn handle_connection<T: io::Read + io::Write>(stream: T) {
    match read_respond_loop(stream) {
        Ok(_) => println!("closed connection"),
        Err(e) => eprintln!("error handling connection: {e}"),
    }
}

fn read_respond_loop<T: io::Read + io::Write>(mut stream: T) -> anyhow::Result<()> {
    let mut read_state = resp::ReadState::new();
    while read_state.can_read_more {
        let object = resp::deserialize_object(&mut read_state, &mut stream)?;
        do_command(&object, &mut stream)?;
    }
    Ok(())
}

fn do_command<T: io::Write>(object: &resp::Object, stream: &mut T) -> anyhow::Result<()> {
    let resp::Object::Array(elements) = object else {
        return Err(anyhow!("Expected an non-empty array."));
    };

    if elements.is_empty() {
        return Err(anyhow!("Expected an non-empty array."));
    }

    let resp::Object::BulkString(command) = &*elements[0] else {
        return Err(anyhow!("Expected first element to be a bulk string."));
    };

    let command = String::from_utf8_lossy(&command);

    match command.as_ref() {
        "PING" => {
            write!(stream, "+PONG\r\n")?;
            Ok(())
        }
        "ECHO" => do_echo(&elements[1..], stream),
        command => Err(anyhow!("unknown command: {}", command)),
    }
}

fn do_echo<T: io::Write>(elements: &[Box<resp::Object>], stream: &mut T) -> anyhow::Result<()> {
    if elements.len() == 1 {
        elements[0].serialize(stream)?;
        Ok(())
    } else {
        Err(anyhow!("ECHO expects only one argument"))
    }
}
