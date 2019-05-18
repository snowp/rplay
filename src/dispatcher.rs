use super::api;
use super::protos;
use super::server;
use std::collections::HashMap;
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

pub struct Dispatcher {
    _receive_thread: JoinHandle<()>,
}

impl Dispatcher {
    pub fn new<F, S, A, T>(
        receiver: Receiver<(Arc<protos::Message>, S)>,
        num_workers: u32,
        f: F,
        api: &A,
    ) -> Self
    where
        F: Send + Sync + 'static + Copy + FnMut(&protos::Message, &mut T) -> protos::Message,
        S: server::MessageSender + Send + Clone + 'static,
        A: api::Api<T>,
        T: Send + 'static,
    {
        let mut ready_channels: Vec<Receiver<bool>> = Vec::new();
        let mut sender_channels: Vec<Sender<(Arc<protos::Message>, S)>> = Vec::new();
        let mut threads: Vec<JoinHandle<()>> = Vec::new();

        let mut handler = f;

        for _ in 0..num_workers {
            let (ready_sender, ready_receiver) = mpsc::channel();
            ready_channels.push(ready_receiver);
            let (work_sender, work_receiver) = mpsc::channel();
            sender_channels.push(work_sender);

            let mut tls_api = api.create_tls_api();
            threads.push(thread::spawn(move || {
                let mut api = tls_api;
                ready_sender.send(true).unwrap();
                loop {
                    match work_receiver.recv() {
                        Ok((msg, sender)) => {
                            let response = handler(&msg, &mut api);
                            sender.send(Arc::new(response)).unwrap();
                            match ready_sender.send(true) {
                                Ok(_) => {}
                                Err(_) => {}
                            }
                        }
                        Err(_) => break,
                    }
                }
            }));
        }

        Dispatcher {
            _receive_thread: thread::spawn(move || {
                let mut stream_sessions: HashMap<u64, usize> = HashMap::new();
                // Keep reading messages off receiver. If the other end is destroyed,
                // simply stop looping as we're about to be clened up.
                loop {
                    match receiver.recv() {
                        Ok((msg, sender)) => {
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
                                        Err(_) => {}
                                    }
                                }
                            } else {
                                sender_channels[session as usize]
                                    .send((msg.clone(), sender.clone()))
                                    .unwrap();
                            }
                        }
                        Err(_) => break,
                    }
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::Api;
    use dispatcher;
    use mpsc::SendError;
    use protos;
    use server::MessageSender;
    use std::result;
    use std::sync::mpsc::Sender;
    use std::sync::{mpsc, Arc, Condvar, Mutex};
    use std::thread;

    struct TestSender {}

    impl Clone for TestSender {
        fn clone(self: &Self) -> Self {
            TestSender {}
        }
    }
    impl MessageSender for TestSender {
        fn send(
            self: &Self,
            _msg: Arc<protos::Message>,
        ) -> result::Result<(), SendError<Arc<protos::Message>>> {
            result::Result::Ok(())
        }
    }

    struct TestApi {
        sender: Sender<(protos::Message, thread::ThreadId)>,
        cvar_pair: Arc<(Mutex<()>, Condvar)>,
    }

    struct TlsTestApi {
        sender: Sender<(protos::Message, thread::ThreadId)>,
        cvar_pair: Arc<(Mutex<()>, Condvar)>,
    }

    impl TlsTestApi {
        fn handle(self: &Self, msg: &protos::Message) {
            if msg.get_method() == "blocked".to_string() {
                let &(ref lock, ref cvar) = &*self.cvar_pair;
                {
                    let l = lock.lock().unwrap();
                    let _ = cvar.wait(l).unwrap();
                }
            }
            self.sender
                .send((msg.clone(), thread::current().id()))
                .unwrap();
        }
    }

    impl Clone for TlsTestApi {
        fn clone(self: &Self) -> Self {
            TlsTestApi {
                sender: self.sender.clone(),
                cvar_pair: self.cvar_pair.clone(),
            }
        }
    }

    impl Api<TlsTestApi> for TestApi {
        fn create_tls_api(self: &Self) -> TlsTestApi {
            TlsTestApi {
                sender: self.sender.clone(),
                cvar_pair: self.cvar_pair.clone(),
            }
        }
    }

    #[test]
    fn verify_receiver() {
        let (api_sender, api_receiver) = mpsc::channel();
        let (sender, receiver) = mpsc::channel();

        let pair = Arc::new((Mutex::new(()), Condvar::new()));
        let api = TestApi {
            sender: api_sender,
            cvar_pair: pair.clone(),
        };
        let handled;
        {
            let _ = dispatcher::Dispatcher::new(
                receiver,
                1,
                move |msg: &protos::Message, api: &mut TlsTestApi| -> protos::Message {
                    api.sender
                        .send((msg.clone(), thread::current().id()))
                        .unwrap();
                    protos::Message::new()
                },
                &api,
            );

            let mut m = protos::Message::new();
            m.set_method("first".to_string());
            sender.send((Arc::new(m), TestSender {})).unwrap();

            let (h, _) = api_receiver.recv().unwrap();
            handled = h;
        }
        assert_eq!(handled.get_method(), "first".to_string());
    }

    #[test]
    fn verify_multithreaded() {
        let (api_sender, api_receiver) = mpsc::channel();
        let (sender, receiver) = mpsc::channel();

        let pair = Arc::new((Mutex::new(()), Condvar::new()));

        let api = TestApi {
            sender: api_sender,
            cvar_pair: pair.clone(),
        };
        let handled;
        let tid1;
        let tid2;
        {
            let _ = dispatcher::Dispatcher::new(
                receiver,
                2,
                move |msg: &protos::Message, api: &mut TlsTestApi| -> protos::Message {
                    api.handle(&msg);
                    protos::Message::new()
                },
                &api,
            );

            {
                let mut m = protos::Message::new();
                m.set_method("blocked".to_string());
                sender.send((Arc::new(m), TestSender {})).unwrap();
            }

            {
                let mut m = protos::Message::new();
                m.set_method("first".to_string());
                sender.send((Arc::new(m), TestSender {})).unwrap();
            }

            // Receive "second", as "first" is blocked on the cond var.
            let (_, t) = api_receiver.recv().unwrap();
            tid2 = t;

            // Keep notifying the condvar until we get something on the receiver;
            // this means that "first" has been unblocked.
            loop {
                let &(ref lock, ref cvar) = &*pair;
                {
                    let _ = lock.lock().unwrap();
                    cvar.notify_one();
                }
                match api_receiver.try_recv() {
                    Ok((h, t)) => {
                        handled = h;
                        tid1 = t;
                        break;
                    }
                    Err(_) => {}
                }
            }
        }

        // Verify that first and second was handled on separate threads.
        assert_eq!(handled.get_method(), "blocked".to_string());
        assert_ne!(tid1, tid2);
    }
}
