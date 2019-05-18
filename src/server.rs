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
use std::result;
use std::sync::mpsc;
use std::sync::mpsc::SendError;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

const SERVER_TOKEN: Token = Token(0);

pub trait MessageSender {
    fn send(
        self: &Self,
        msg: Arc<protos::Message>,
    ) -> result::Result<(), SendError<Arc<protos::Message>>>;
}

pub struct SendMessage {
    sender: Sender<WriterEvent>,
    token: Token,
}

impl MessageSender for SendMessage {
    fn send(
        self: &Self,
        msg: Arc<protos::Message>,
    ) -> result::Result<(), SendError<Arc<protos::Message>>> {
        match self
            .sender
            .send(WriterEvent::WriteData((self.token, msg.clone())))
        {
            Ok(()) => Ok(()),
            Err(_) => Err(SendError(msg.clone())),
        }
    }
}

impl Clone for SendMessage {
    fn clone(&self) -> Self {
        SendMessage {
            token: self.token,
            sender: self.sender.clone(),
        }
    }
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

enum WriterEvent {
    NewConnection((Token, TcpStream, SocketAddr)),
    WriteData((Token, Arc<protos::Message>)),
}

pub struct Server {
    sessions: HashMap<Token, (TcpStream, SocketAddr)>,
    tcp_listener: TcpListener,
    listeners: Vec<Sender<(Arc<protos::Message>, SendMessage)>>,
    poll: Poll,
    writer_sender: Sender<WriterEvent>,
    _writer_thread: JoinHandle<()>,
}

impl Server {
    pub fn new(addr: &String) -> Self {
        let a = addr.parse().unwrap();
        let (writer_sender, writer_receiver) = mpsc::channel();

        let ss = HashMap::new();
        let s = Server {
            sessions: ss,
            tcp_listener: TcpListener::bind(&a).unwrap(),
            listeners: Vec::new(),
            poll: Poll::new().unwrap(),
            writer_sender: writer_sender,
            _writer_thread: thread::spawn(move || {
                let mut sessions = HashMap::new();
                loop {
                    match writer_receiver.recv().unwrap() {
                        WriterEvent::NewConnection((token, stream, addr)) => {
                            println!("got connetion {:?}", token);
                            sessions.insert(token, (stream, addr));
                        }
                        WriterEvent::WriteData((token, msg)) => {
                            println!("got data {:?}", token);
                            let (ref mut stream, _) = sessions.get_mut(&token).unwrap();
                            stream
                                .write_bufs(&[msg.write_to_bytes().unwrap().as_slice().into()])
                                .unwrap();
                        }
                    }
                }
            }),
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

    fn insert_with_next_token(self: &mut Self, (session, addr): (TcpStream, SocketAddr)) -> Token {
        let token = next_token(&self.sessions);

        // Register events to poll for, and notify the sender thread about this connection.
        self.poll
            .register(&session, token, Ready::readable(), PollOpt::edge())
            .unwrap();
        self.writer_sender
            .send(WriterEvent::NewConnection((
                token,
                session.try_clone().unwrap(),
                addr,
            )))
            .unwrap();

        self.sessions.insert(token, (session, addr));
        token
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
                            self.insert_with_next_token((s, a));
                        }
                        Err(e) => println!("failed to accept: {}", e),
                    }
                } else {
                    let (ref mut stream, _) = self.sessions.get_mut(&event.token()).unwrap();
                    let mut buffer = [0; 256];
                    match read_chunk(&mut buffer, stream) {
                        Ok(0) => println!("event with no output"),
                        Ok(size) => match buffer.chunks(size).next() {
                            Some(chunk) => {
                                println!("received {} bytes", size);
                                let mut cis = protobuf::stream::CodedInputStream::from_bytes(chunk);
                                let mut m = protos::Message::new();
                                match m.merge_from(&mut cis) {
                                    Ok(_) => {}
                                    Err(e) => {
                                        println!("failed to parse message: {}", e);
                                        println!("{:?}", chunk);
                                    }
                                }

                                let marc = Arc::new(m);
                                for l in self.listeners.iter() {
                                    l.send((
                                        marc.clone(),
                                        SendMessage {
                                            token: event.token(),
                                            sender: self.writer_sender.clone(),
                                        },
                                    )).unwrap();
                                }
                            }
                            None => println!("failed to read {} bytes", size),
                        },
                        Err(e) => {
                            if e.kind() == ErrorKind::WouldBlock
                                || e.kind() == ErrorKind::ConnectionReset
                            {
                                continue;
                            } else {
                                println!("error reading: {}", e);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn add_listener(self: &mut Self, l: Sender<(Arc<protos::Message>, SendMessage)>) {
        self.listeners.push(l);
    }
}
