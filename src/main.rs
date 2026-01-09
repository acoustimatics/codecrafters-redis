//! Code Crafters build a Redis challenge

mod engine;
mod resp;

use std::collections::HashMap;
use std::io;
use std::net;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread;

fn main() {
    let mut connection_id = 0;
    match net::TcpListener::bind("127.0.0.1:6379") {
        Ok(listener) => {
            // Create channels to make requests of the engine.
            let (tx_req, rx_req) = mpsc::channel();
            // Start up a thread running the data engine.
            thread::spawn(move || {
                run_engine(rx_req);
            });
            // Start listening for incomming connections.
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        connection_id += 1;
                        println!("[id={connection_id}] accepted new connection");
                        let tx_req = tx_req.clone();
                        thread::spawn(move || handle_connection(connection_id, stream, tx_req));
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

/// Handles all the interactions for a connection.
fn handle_connection<T: io::Read + io::Write>(
    id: ConnectionId,
    stream: T,
    tx_req: mpsc::Sender<Request>,
) {
    // Tell engine where to send responses.
    let (tx_res, rx_res) = mpsc::channel();
    let req = Request::new_sender(id, tx_res);
    if let Err(e) = tx_req.send(req) {
        eprintln!("{e}");
        return;
    }
    // TODO: Check the response.
    if let Err(e) = rx_res.recv() {
        eprintln!("{e}");
        return;
    }

    match read_request_respond_loop(id, stream, &tx_req, &rx_res) {
        Ok(_) => println!("[id={id}] closed connection"),
        Err(e) => eprintln!("{e}"),
    }

    let req = Request::new_done(id);
    if let Err(e) = tx_req.send(req) {
        eprintln!("{e}");
        return;
    }
    // TODO: Check the response.
    if let Err(e) = rx_res.recv() {
        eprintln!("{e}");
        return;
    }
}

/// Goes into a loop of reading commands from the client, requesting the
/// engine do the command, and then sending the result back to the client.
fn read_request_respond_loop<T: io::Read + io::Write>(
    id: ConnectionId,
    mut stream: T,
    tx_req: &mpsc::Sender<Request>,
    rx_res: &mpsc::Receiver<Response>,
) -> anyhow::Result<()> {
    let mut read_state = resp::ReadState::new();
    while read_state.can_read_more {
        let command = resp::deserialize_object(&mut read_state, &mut stream)?;
        let req = Request::new_command(id, command);
        tx_req.send(req)?;
        match rx_res.recv()? {
            Response::Ok => write!(stream, "+OK\r\n")?,
            Response::Return(res) => resp::serialize(&mut stream, &res)?,
        }
    }
    Ok(())
}

/// `fn` run in the engine thread.
fn run_engine(rx_req: Receiver<Request>) {
    // Create the engine object.
    let mut engine = engine::Engine::new();
    // A map of senders to send responses to.
    let mut senders = HashMap::new();
    // Start request processing loop.
    for req in rx_req {
        // Handle the request.
        let res = match req.value {
            RequestValue::Command(object) => {
                let res = engine.do_command(object);
                Response::Return(res)
            }
            RequestValue::Sender(tx_res) => {
                senders.insert(req.id, tx_res);
                Response::Ok
            }
            RequestValue::Done => {
                let _ = senders.remove(&req.id);
                Response::Ok
            }
        };
        // Try to respond to the request.
        if let Some(tx_res) = senders.get(&req.id) {
            if let Err(e) = tx_res.send(res) {
                // TODO: Do more in response to the error?
                eprintln!("error responding to request: {e}");
            }
        }
    }
}

/// A request to make of the engine thread.
struct Request {
    /// Id of connection making the request.
    id: ConnectionId,

    /// The content of the request.
    value: RequestValue,
}

impl Request {
    /// Creates a new command request.
    fn new_command(id: ConnectionId, command: engine::Object) -> Self {
        let value = RequestValue::Command(command);
        Self { id, value }
    }

    /// Creates a new sender request.
    fn new_sender(id: ConnectionId, sender: mpsc::Sender<Response>) -> Self {
        let value = RequestValue::Sender(sender);
        Self { id, value }
    }

    /// Creates a new done request.
    fn new_done(id: ConnectionId) -> Self {
        let value = RequestValue::Done;
        Self { id, value }
    }
}

/// Content of a request.
enum RequestValue {
    /// Send a command to the engine.
    Command(engine::Object),
    /// Gives the engine thread a channel on which responses can be
    /// sent to a connection.
    Sender(mpsc::Sender<Response>),
    /// Tells the engine thread the connection is done so the engine
    /// thread can drop resources.
    Done,
}

/// A response returned for a request to the engine thread.
enum Response {
    /// A generic OK.
    Ok,
    /// An object to return back to a connection's client.
    Return(engine::Object),
}

/// An ID assigned to a connection.
type ConnectionId = usize;
