use std::io;
use std::io::prelude::*;
use std::net;
use std::thread;

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

fn handle_connection<T: Read + Write>(stream: T) {
    match read_respond_loop(stream) {
        Ok(_) => println!("closed connection"),
        Err(e) => eprintln!("error handling connection: {e}"),
    }
}

fn read_respond_loop<T: Read + Write>(mut stream: T) -> io::Result<()> {
    let mut buffer: [u8; 256] = [0; 256];
    let mut n_bytes_read = stream.read(&mut buffer)?;
    while n_bytes_read > 0 {
        write!(stream, "+PONG\r\n")?;
        n_bytes_read = stream.read(&mut buffer)?;
    }
    Ok(())
}
