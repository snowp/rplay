use super::protos;
use super::server;
use mio::Token;
use std::boxed::Box;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub struct Dispatcher {
    receive_thread: JoinHandle<()>,
}

impl Dispatcher {
    pub fn new<F>(
        receiver: Receiver<(Arc<protos::Message>, Token, Sender<server::WriterEvent>)>,
        num_workers: u32,
        f: F,
    ) -> Self
    where
        F: Send + Sync + 'static + Fn(&protos::Message) -> protos::Message,
    {
        let mut ready_channels: Vec<Receiver<bool>> = Vec::new();
        let mut sender_channels: Vec<
            Sender<(Arc<protos::Message>, Token, Sender<server::WriterEvent>)>,
        > = Vec::new();
        let mut threads: Vec<JoinHandle<()>> = Vec::new();

        let farc = Arc::new(f);
        for _ in 1..num_workers {
            let (ready_sender, ready_receiver) = mpsc::channel();
            ready_channels.push(ready_receiver);
            let (work_sender, work_receiver) = mpsc::channel();
            sender_channels.push(work_sender);

            let farcc = farc.clone();
            threads.push(thread::spawn(move || {
                ready_sender.send(true).unwrap();
                loop {
                    match work_receiver.recv() {
                        Ok((msg, token, sender)) => {
                            let response = farcc(&msg);
                            sender
                                .send(server::WriterEvent::WriteData((token, Arc::new(response))));
                            ready_sender.send(true).unwrap();
                        }
                        Err(e) => println!("got error while waiting for work {}", e),
                    }
                }
            }));
        }

        Dispatcher {
            receive_thread: thread::spawn(move || loop {
                let msg = receiver.recv().unwrap();
                for (i, w) in ready_channels.iter().enumerate() {
                    match w.try_recv() {
                        Ok(_) => {
                            sender_channels[i].send(msg.clone()).unwrap();
                            break;
                        }
                        Err(_) => (),
                    }
                }
            }),
        }
    }
}
