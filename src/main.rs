use std::net;

fn main() {
    match net::TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            for stream in listener.incoming() {
                match stream {
                    Ok(_stream) => {
                        println!("accepted new connection");
                    }
                    Err(e) => {
                        println!("error accepting connection {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("error binding TCP listener: {}", e);
        }
    }
}
