use std::io;
use std::io::Write;
use std::net;

fn main() {
    match net::TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            for stream in listener.incoming() {
                match stream {
                    Ok(mut stream) => {
                        println!("accepted new connection");
                        match respond_to_connection(&mut stream) {
                            Ok(_) => println!("closed connection"),
                            Err(e) => eprintln!("error handling connection: {e}"),
                        }
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

fn respond_to_connection(stream: &mut net::TcpStream) -> io::Result<()> {
    write!(stream, "+PONG\r\n")
}
