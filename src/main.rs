extern crate mio;
extern crate protobuf;
mod dispatcher;
mod protos;
mod server;

use std::env;
use std::sync::mpsc;

fn print_message(msg: &protos::Message) {
    println!(
        "got message: {}",
        String::from_utf8(Vec::from(msg.get_body())).unwrap()
    )
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut server = server::Server::new(&args[1]);

    let (s, r) = mpsc::channel();
    server.add_listener(s);

    let _ = dispatcher::Dispatcher::new(r, 32, &print_message);
    server.start();
}
