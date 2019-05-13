extern crate mio;
extern crate protobuf;
mod client;
mod dispatcher;
mod protos;
mod server;

use protobuf::Message;
use std::env;
use std::sync::mpsc;
use std::thread;
use std::time;

fn print_message(msg: &protos::Message) -> protos::Message {
    if msg.get_annotations()["name"] == "call" {
        let mut p = protos::Ping::new();
        let mut cos = protobuf::CodedInputStream::from_bytes(msg.get_body());
        p.merge_from(&mut cos).unwrap();
        thread::sleep(time::Duration::from_millis(10));

        println!(
            "received ping with data {:?} on thread {:?}",
            p.get_data(),
            thread::current().id()
        );
    }
    let mut p = protos::Pong::new();
    p.set_data("hi from server".to_string());
    let mut m = protos::Message::new();
    m.set_body(p.write_to_bytes().unwrap());
    m
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args[1] == "client" {
        let mut ping = protos::Ping::new();
        ping.set_data("hello server".to_string());

        client::Client::send(&args[2], &String::from("call"), &ping);
    }

    if args[1] == "server" {
        let mut server = server::Server::new(&args[2]);
        let (s, r) = mpsc::channel();
        server.add_listener(s);

        let _ = dispatcher::Dispatcher::new(r, 32, &print_message);
        server.start();
    }
}
