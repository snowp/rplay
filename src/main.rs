extern crate mio;
extern crate protobuf;
mod server;
mod protos;

use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut s = server::Server::new(&args[1]);
    s.start();
}
