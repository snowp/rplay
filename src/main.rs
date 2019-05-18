extern crate mio;
extern crate protobuf;
mod api;
mod client;
mod dispatcher;
mod protos;
mod redis_api;
mod server;

use std::env;
use std::sync::mpsc;

fn handler<'a, F, S, T, A>(msg: &protos::Message, f: F, api: &'a mut A) -> protos::Message
where
    F: Fn(&S, &'a A) -> T,
    S: protobuf::Message,
    T: protobuf::Message,
    A: 'a,
{
    let mut request = S::new();
    let mut cos = protobuf::CodedInputStream::from_bytes(msg.get_body());
    request.merge_from(&mut cos).unwrap();

    let mut m = protos::Message::new();
    {
        let response = f(&request, api);
        m.set_body(response.write_to_bytes().unwrap());
    }

    m
}

macro_rules! handle {
    ($t:ty, $name1:expr => $handler1:expr, $($name:expr => $handler:expr),*) => {{
        |msg: &protos::Message, api: &mut $t| -> protos::Message {
         match msg.get_method() {
            $name1 => handler(msg, $handler1, api),
            $(
                $name => handler(msg, $handler, api),
            )*
            _ => protos::Message::new(),
        }}}
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

        let api = redis_api::RedisApi {
            addr: "127.0.0.1:6379".to_string(),
        };
        let _ = dispatcher::Dispatcher::new(
            r,
            4,
            handle!{
                redis_api::RedisTlsApi,
                "Echo" => |_request: &protos::Ping, _api| -> protos::Pong { protos::Pong::new() },
                "Bara" => |_request: &protos::Ping, _api| -> protos::Pong { protos::Pong::new() }
            },
            &api,
        );
        server.start();
    }
}
