extern crate mio;
extern crate protobuf;
mod client;
mod dispatcher;
mod protos;
mod server;

use std::env;
use std::sync::mpsc;

fn handler<F, S, T>(msg: &protos::Message, f: F) -> protos::Message
where
    F: Fn(&S) -> T,
    S: protobuf::Message,
    T: protobuf::Message,
{
    let mut request = S::new();
    let mut cos = protobuf::CodedInputStream::from_bytes(msg.get_body());
    request.merge_from(&mut cos).unwrap();

    let response = f(&request);
    let mut m = protos::Message::new();
    m.set_body(response.write_to_bytes().unwrap());

    m
}

macro_rules! handle {
    ($name1:expr => $handler1:expr, $($name:expr => $handler:expr),*) => {{
        |msg| match msg.get_method() {
            $name1 => handler(msg, $handler1),
            $(
                $name => handler(msg, $handler),
            )*
            _ => protos::Message::new(),
        }}
    };
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

        let _ = dispatcher::Dispatcher::new(
            r,
            32,
            handle!{
                "Echo" => |_request: &protos::Ping| -> protos::Pong { protos::Pong::new() },
                "Bara" => |_request: &protos::Ping| -> protos::Pong { protos::Pong::new() }
            },
        );
        server.start();
    }
}
