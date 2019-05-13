extern crate protobuf;

use super::protos;
use mio::tcp::TcpStream;
use mio::*;
use protobuf::Message;

pub struct Client {}

impl Client {
    pub fn send<T: protobuf::Message>(addr: &String, name: &String, msg: &T) {
        let poll = Poll::new().unwrap();
        let stream = TcpStream::connect(&addr.parse().unwrap()).unwrap();
        poll.register(
            &stream,
            Token(0),
            Ready::writable() | Ready::readable(),
            PollOpt::edge(),
        ).unwrap();

        let mut events = Events::with_capacity(1024);
        let mut sent = false;
        loop {
            poll.poll(&mut events, None).unwrap();

            for e in events.iter() {
                if !sent && e.readiness().is_writable() {
                    let data = msg.write_to_bytes().unwrap();
                    let mut wrapper = protos::Message::new();
                    wrapper.set_body(data);
                    wrapper
                        .mut_annotations()
                        .insert(String::from("name"), name.to_string());
                    let msg_bytes = wrapper.write_to_bytes().unwrap();
                    let buffers: [&IoVec; 1] = [msg_bytes.as_slice().into()];
                    stream.write_bufs(&buffers).unwrap();
                    sent = true;
                }
                if e.readiness().is_readable() {
                    let mut buffer = vec![0; 256];
                    let mut size: usize;
                    {
                        {
                            let mut buffer_ref: &mut [u8] = &mut buffer;
                            let mut buffers: [&mut IoVec; 1] = [buffer_ref.into()];
                            size = stream.read_bufs(&mut buffers).unwrap();
                            println!("received {} bytes", size);
                        }
                        buffer.truncate(size);
                        let mut wrapper = protos::Message::new();
                        let mut cis = protobuf::CodedInputStream::from_bytes(&buffer);
                        wrapper.merge_from(&mut cis).unwrap();

                        let mut ping = protos::Ping::new();
                        let mut cis = protobuf::CodedInputStream::from_bytes(wrapper.get_body());
                        ping.merge_from(&mut cis).unwrap();
                        println!("received ping with data {}", ping.get_data());
                    }
                }
            }
        }
    }
}
