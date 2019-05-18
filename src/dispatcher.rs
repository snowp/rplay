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
    use std::sync::mpsc::{Receiver, Sender};
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

    struct TestDispatcer {
        _dispatcher: dispatcher::Dispatcher,
        dispatch_sender: Sender<(Arc<protos::Message>, TestSender)>,
        test_receiver: Receiver<(protos::Message, thread::ThreadId)>,
        condvar_pair: Arc<(Mutex<()>, Condvar)>,
    }

    impl TestDispatcer {
        fn new(num_workers: u32) -> TestDispatcer {
            let (api_sender, api_receiver) = mpsc::channel();
            let (sender, receiver) = mpsc::channel();

            let pair = Arc::new((Mutex::new(()), Condvar::new()));
            let api = TestApi {
                sender: api_sender,
                cvar_pair: pair.clone(),
            };
            TestDispatcer {
                _dispatcher: dispatcher::Dispatcher::new(
                    receiver,
                    num_workers,
                    move |msg: &protos::Message, api: &mut TlsTestApi| -> protos::Message {
                        api.handle(&msg);
                        protos::Message::new()
                    },
                    &api,
                ),
                dispatch_sender: sender,
                test_receiver: api_receiver,
                condvar_pair: pair,
            }
        }

        fn dispatch_msg(self: &Self, msg: &protos::Message) {
            self.dispatch_sender
                .send((Arc::new(msg.clone()), TestSender {}))
                .unwrap();
        }

        fn recv_handled(self: &Self) -> (protos::Message, thread::ThreadId) {
            self.test_receiver.recv().unwrap()
        }

        fn handle_blocked(self: &Self) -> (protos::Message, thread::ThreadId) {
            loop {
                let &(ref lock, ref cvar) = &*self.condvar_pair;
                {
                    let _ = lock.lock().unwrap();
                    cvar.notify_one();
                }
                match self.test_receiver.try_recv() {
                    Ok((h, t)) => {
                        return (h, t);
                    }
                    Err(_) => {}
                }
            }
        }
    }

    #[test]
    fn verify_receiver() {
        let test_dispatcher = TestDispatcer::new(1);
        {
            let mut m = protos::Message::new();
            m.set_method("first".to_string());
            test_dispatcher.dispatch_msg(&m);
        }

        let (h, _) = test_dispatcher.recv_handled();
        assert_eq!(h.get_method(), "first".to_string());
    }

    #[test]
    fn verify_multithreaded() {
        let test_dispatcher = TestDispatcer::new(2);

        {
            let mut m = protos::Message::new();
            m.set_method("blocked".to_string());
            test_dispatcher.dispatch_msg(&m);
        }

        {
            let mut m = protos::Message::new();
            m.set_method("not blocked".to_string());
            test_dispatcher.dispatch_msg(&m);
        }

        // We can immediately receive the message that wasn't blocked.
        let (h1, t1) = test_dispatcher.recv_handled();
        assert_eq!(h1.get_method(), "not blocked".to_string());

        // Trigger the cond var and receive the blocked message.
        let (h2, t2) = test_dispatcher.handle_blocked();
        assert_eq!(h2.get_method(), "blocked".to_string());

        // Verify that first and second was handled on separate threads.
        assert_ne!(t1, t2);
    }
}
