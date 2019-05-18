use super::api;
use super::protos;
use super::server;
use api::Api;
use server::MessageSender;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub struct Dispatcher {
    receive_thread: JoinHandle<()>,
}

impl Dispatcher {
    pub fn new<F, S, A, T>(
        receiver: Receiver<(Arc<protos::Message>, S)>,
        num_workers: u32,
        f: F,
        api: &A,
    ) -> Self
    where
        F: Send + Sync + 'static + Copy + Fn(&protos::Message, &T) -> protos::Message,
        S: server::MessageSender + Send + Clone + 'static,
        A: api::Api<T>,
        T: Send + Sync + 'static,
    {
        let mut ready_channels: Vec<Receiver<bool>> = Vec::new();
        let mut sender_channels: Vec<Sender<(Arc<protos::Message>, S)>> = Vec::new();
        let mut threads: Vec<JoinHandle<()>> = Vec::new();

        for _ in 1..num_workers {
            let (ready_sender, ready_receiver) = mpsc::channel();
            ready_channels.push(ready_receiver);
            let (work_sender, work_receiver) = mpsc::channel();
            sender_channels.push(work_sender);

            let tls_api = api.create_tls_api();
            threads.push(thread::spawn(move || {
                ready_sender.send(true).unwrap();
                loop {
                    match work_receiver.recv() {
                        Ok((msg, sender)) => {
                            let response = f(&msg, &tls_api);
                            sender.send(Arc::new(response)).unwrap();
                            ready_sender.send(true).unwrap();
                        }
                        Err(e) => println!("got error while waiting for work {}", e),
                    }
                }
            }));
        }

        Dispatcher {
            receive_thread: thread::spawn(move || {
                let mut stream_sessions: HashMap<u64, usize> = HashMap::new();
                loop {
                    let (msg, sender) = receiver.recv().unwrap();
                    let session = msg.as_ref().get_session();
                    if session == 0 {
                        let mut s = 0;
                        while stream_sessions.contains_key(&s) {
                            s += 1;
                        }
                        for (i, w) in ready_channels.iter().enumerate() {
                            match w.try_recv() {
                                Ok(_) => {
                                    stream_sessions.insert(s, i);
                                    sender_channels[i]
                                        .send((msg.clone(), sender.clone()))
                                        .unwrap();
                                    break;
                                }
                                Err(_) => (),
                            }
                        }
                    } else {
                        sender_channels[session as usize]
                            .send((msg.clone(), sender.clone()))
                            .unwrap();
                    }
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use dispatcher;
    use mpsc::SendError;
    use protos;
    use server;
    use server::MessageSender;
    use std::result;
    use std::sync::mpsc::{Receiver, Sender};
    use std::sync::{mpsc, Arc};

    struct TestSender {}

    impl Clone for TestSender {
        fn clone(self: &Self) -> Self {
            TestSender {}
        }
    }
    impl MessageSender for TestSender {
        fn send(
            self: &Self,
            msg: Arc<protos::Message>,
        ) -> result::Result<(), SendError<Arc<protos::Message>>> {
            result::Result::Ok(())
        }
    }

    #[test]
    fn verify_receiver() {
        let (sender, receiver) = mpsc::channel();
        let (handler_sender, handler_receiver) = mpsc::channel();

        let d = dispatcher::Dispatcher::new(
            receiver,
            1,
            move |msg: &protos::Message| -> protos::Message {
                handler_sender.send(Arc::new(msg.clone()));
                protos::Message::new()
            },
        );

        let mut m = protos::Message::new();
        m.set_method("hello".to_string());

        sender.send((Arc::new(m), TestSender {}));

        let mut handled = handler_receiver.recv().unwrap();
        assert_eq!(handled.get_method(), m.get_method());
    }
}
