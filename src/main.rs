extern crate mio;
extern crate protobuf;
mod client;
mod dispatcher;
mod protos;
mod server;

use std::env;
use std::sync::mpsc;

fn print_message(msg: &protos::Message) {
    println!("got message: {:?}", msg.get_body());
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args[1] == "client" {
        let mut m = protos::Message::new();
        m.set_body(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);

        client::Client::send(&args[2], &m);
    }

    if args[1] == "server" {
        let mut server = server::Server::new(&args[2]);
        let (s, r) = mpsc::channel();
        server.add_listener(s);

        let _ = dispatcher::Dispatcher::new(r, 32, &print_message);
        server.start();
    }
}
