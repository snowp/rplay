extern crate mio;
use mio::net::{TcpListener, TcpStream};
use mio::*;
use std::collections::HashMap;
use std::env;
use std::io::ErrorKind;
use std::io::Result;
use std::net::SocketAddr;

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

fn main() {
    let server_token = Token::from(0);
    let args: Vec<String> = env::args().collect();
    let addr = args[1].parse().unwrap();
    let server = TcpListener::bind(&addr).unwrap();
    let poll = Poll::new().unwrap();
    poll.register(&server, Token(0), Ready::readable(), PollOpt::edge())
        .unwrap();

    let mut events = Events::with_capacity(1024);

    let mut sessions: HashMap<Token, (TcpStream, SocketAddr)> = HashMap::new();
    loop {
        poll.poll(&mut events, None).unwrap();

        for event in events.iter() {
            if event.token() == server_token {
                let a = server.accept();
                match a {
                    Ok((s, a)) => {
                        let response = String::from("there you are\n");
                        let buffers: [&IoVec; 1] = [response.as_str().as_bytes().into()];
                        s.write_bufs(&buffers).unwrap();
                        let token = next_token(&sessions);
                        poll.register(&s, token, Ready::readable(), PollOpt::edge())
                            .unwrap();
                        sessions.insert(token, (s, a));
                    }
                    Err(e) => println!("failed to accept: {}", e),
                }
            } else {
                match sessions.get_mut(&event.token()) {
                    Some((ref mut s, ref a)) => {
                        let mut buffer = [0; 256];
                        match read_chunk(&mut buffer, s) {
                            Ok(0) => println!("event with no output"),
                            Ok(size) => match buffer.chunks(size).next() {
                                Some(chunk) => println!(
                                    "received data {} from {}",
                                    String::from_utf8(Vec::from(chunk)).unwrap(),
                                    a
                                ),
                                None => println!("failed to read {} bytes", size),
                            },
                            Err(e) => {
                                if e.kind() == ErrorKind::WouldBlock {
                                    continue;
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
