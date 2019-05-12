extern crate mio;
extern crate protobuf;

use super::protos;
use mio::net::{TcpListener, TcpStream};
use mio::*;
use protobuf::Message;
use std::collections::HashMap;
use std::io::ErrorKind;
use std::io::Result;
use std::net::SocketAddr;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;

const SERVER_TOKEN: Token = Token(0);

pub struct Server {
    sessions: HashMap<Token, (TcpStream, SocketAddr)>,
    tcp_listener: TcpListener,
    listeners: Vec<Sender<Arc<protos::Message>>>,
    poll: Poll,
}

fn next_token(sessions: &HashMap<Token, (TcpStream, SocketAddr)>) -> Token {
    let mut t = 1;

    loop {
        if sessions.contains_key(&Token::from(t)) {
            t = t + 1;
        } else {
            break;
        }
    }

    return Token::from(t);
}

fn read_chunk(buffer: &mut [u8], stream: &mut TcpStream) -> Result<(usize)> {
    let mut buffers: [&mut IoVec; 1] = [buffer.into()];

    match stream.read_bufs(&mut buffers) {
        Ok(size) => Ok(size),
        Err(e) => Err(e),
    }
}
impl Server {
    pub fn new(addr: &String) -> Self {
        let a = addr.parse().unwrap();
        let s = Server {
            sessions: HashMap::new(),
            tcp_listener: TcpListener::bind(&a).unwrap(),
            listeners: Vec::new(),
            poll: Poll::new().unwrap(),
        };
        s.poll
            .register(
                &s.tcp_listener,
                SERVER_TOKEN,
                Ready::readable(),
                PollOpt::edge(),
            )
            .unwrap();
        s
    }

    pub fn start(self: &mut Self) {
        let mut events = Events::with_capacity(1024);
        loop {
            self.poll.poll(&mut events, None).unwrap();

            for event in events.iter() {
                if event.token() == SERVER_TOKEN {
                    let a = self.tcp_listener.accept();
                    match a {
                        Ok((s, a)) => {
                            let response = String::from("there you are\n");
                            let buffers: [&IoVec; 1] = [response.as_str().as_bytes().into()];
                            s.write_bufs(&buffers).unwrap();
                            let token = next_token(&self.sessions);
                            self.poll
                                .register(&s, token, Ready::readable(), PollOpt::edge())
                                .unwrap();
                            self.sessions.insert(token, (s, a));
                        }
                        Err(e) => println!("failed to accept: {}", e),
                    }
                } else {
                    match self.sessions.get_mut(&event.token()) {
                        Some((ref mut s, ref a)) => {
                            let mut buffer = [0; 256];
                            match read_chunk(&mut buffer, s) {
                                Ok(0) => println!("event with no output"),
                                Ok(size) => match buffer.chunks(size).next() {
                                    Some(chunk) => {
                                        let mut cis =
                                            protobuf::stream::CodedInputStream::from_bytes(chunk);
                                        let mut m = protos::Message::new();
                                        m.merge_from(&mut cis).unwrap();

                                        let marc = Arc::new(m);

                                        for l in self.listeners.iter() {
                                            l.send(marc.clone()).unwrap();
                                        }
                                    }
                                    None => println!("failed to read {} bytes", size),
                                },
                                Err(e) => {
                                    if e.kind() == ErrorKind::WouldBlock {
                                        continue;
                                    } else {
                                        println!("error reading: {}", e);
                                    }
                                }
                            }
                        }
                        None => {}
                    }
                }
            }
        }
    }

    pub fn add_listener(self: &mut Self, l: Sender<Arc<protos::Message>>) {
        self.listeners.push(l);
    }
}
