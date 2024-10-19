use super::key::Key;
use super::key::KEY_LEN;

use std::net::UdpSocket;
use std::sync::mpsc;
use std::sync::{Arc, Mutex}; 
use std::collections::HashMap;
use std::thread;
use std::fmt::{Binary, Debug, Error, Formatter};
use std::str;

use serde::{Deserialize, Serialize};

#[derive(Clone, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct NodeDistance(pub Key, pub Distance, pub String);


impl PartialEq for NodeDistance {
    fn eq(&self, other: &NodeDistance) -> bool {
        let mut equal = true;
        let mut i = 0;
        while equal && i < 32 {
            if self.1 .0[i] != other.1 .0[i] {
                equal = false;
            }

            i += 1;
        }

        equal
    }
}

impl PartialOrd for NodeDistance {
    fn partial_cmp(&self, other: &NodeDistance) -> Option<std::cmp::Ordering> {
        Some(other.1.cmp(&self.1))
    }
}

impl Ord for NodeDistance {
    fn cmp(&self, other: &NodeDistance) -> std::cmp::Ordering {
        other.1.cmp(&self.1)
    }
}


// #[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[derive(Clone, Serialize, Deserialize, Hash, Ord, PartialOrd, Eq, PartialEq, Copy)]
pub struct Distance(pub [u8; KEY_LEN]);

impl Distance {
    pub fn new(k1: &Key, k2: &Key) -> Distance {
        let mut ret = [0; KEY_LEN];
        for i in 0..KEY_LEN {
            ret[i] = k1.0[i] ^ k2.0[i];
        }

        Self(ret)
    }
}

impl Debug for Distance {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        for x in &self.0 {
            write!(f, "{:X}", x)
                .expect("[FAILED] Distance::Debug --> Failed to format contents of Key");
        }
        Ok(())
    }
}

impl Binary for Distance {
    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
        for x in &self.0 {
            write!(f, "{:b}", x)
                .expect("[FAILED] Key::Binary --> Failed to format contents of Distance");
        }
        Ok(())
    }
}


pub const BUF_SIZE: usize = 4096 * 2;
pub const TIMEOUT: u64 = 5000;


#[derive(Debug, Serialize, Deserialize)]
pub enum ValueResult {
    Nodes(Vec<NodeDistance>),
    Value(String),
}


#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ping, 
    FindNode(Vec<NodeDistance>),
    FindNodeValue(ValueResult),
}

#[derive(Clone, Debug)]
pub struct Node {
   pub id: Key,
   pub  ip: String,
   pub port: u8,
   pub socket: Arc<UdpSocket>,
   pub addr: String,
   pub pending: Arc<Mutex<HashMap<Key, mpsc::Sender<Option<Response>>>>>
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RequestType {
    Ping,
    Store(String, String),
    FindNode(Key),
    FindValue(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub enum Message {
    Abort,
    Request(RequestType),
    Response(Response),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    pub src: String,
    pub req: RequestType,
    pub token: Key,

}

#[derive(Serialize, Deserialize, Debug)]
pub struct RpcMessage {
    pub token: Key,
    pub src: String,
    pub dst: String,
    pub msg: Message,
}



impl Node {

    pub fn new(ip: String, port: u8) -> Self {
        let addr = format!("{}:{}", ip, port);
        let id = Key::new(addr.clone());
        let socket = UdpSocket::bind(addr.clone())
            .expect("[FAILED] Rpc::new --> Error while binding UdpSocket to specified addr");

        Node {
            id,
            ip,
            port,
            addr,
            socket: Arc::new(socket),
            pending: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    
    pub fn start(node: Node, sender: mpsc::Sender<Request>) {
        thread::spawn(move || {
            let mut buf = [0u8; BUF_SIZE];
            loop {
                let (len, _) = node.
                    socket.recv_from(&mut buf)
                    .expect("[FAILED] Node::start --> Failed to receive data from peer");
                let payload =
                String::from(str::from_utf8(&buf[..len]).expect(
                    "[FAILED] Node::start --> Unable to parse string from received bytes",
                ));

                let decoded: RpcMessage = serde_json::from_str(&payload)
                .expect("[FAILED] Rpc::open, serde_json --> Unable to decode string payload");

                if decoded.dst != node.addr {
                    eprintln!("[WARNING] Rpc::open --> Destination address doesn't match node address, ignoring");
                    continue;
                }

                match decoded.msg {
                    Message::Abort => {
                        break;
                    }
                    Message::Request(req) => {
                        let wrapped_req = Request {
                            src: decoded.src,
                            token: decoded.token,
                            req,
                        };
                        if let Err(_) = sender.send(wrapped_req) {
                            eprintln!("[FAILED] Node::start, Request --> Receiver is dead, closing channel.");
                            break;
                        }
                    }
                    Message::Response(res) => {
                        node.clone().handle_response(decoded.token, res);
                    }
                }



            }

        });
    }

    pub fn handle_response(self, token: Key, res: Response) {
        thread::spawn(move || {
            let mut pending = self
                .pending
                .lock()
                .expect("[FAILED] Node::handle_response --> Failed to acquire lock on Pending");

            let tmp = match pending.get(&token) {
                Some(sender) => sender.send(Some(res)),
                None => {
                    eprintln!(
                        "[WARNING] Node::handle_response --> Unsolicited response received, ignoring..."
                    );
                    return;
                }
            };

            if let Ok(_) = tmp {
                pending.remove(&token);
            }
        });
    }

    pub fn send_msg(&self, msg: &RpcMessage) {
        let encoded = serde_json::to_string(msg)
            .expect("[FAILED] Rpc::send_msg --> Unable to serialize message");
        self.socket
            .send_to(&encoded.as_bytes(), &msg.dst)
            .expect("[FAILED] Rpc::send_msg --> Error while sending message to specified address");
    }

    pub fn make_req(&self, req: RequestType, _dst: Key, addr: String) -> mpsc::Receiver<Option<Response>> {
        let (sender, rec) = mpsc::channel();
        let mut pending = self
            .pending
            .lock()
            .expect("[FAILED] Rpc::make_request --> Failed to acquire mutex on Pending");

            let token = Key::new(format!(
                "{}:{}:{:?}",
                self.addr,
                addr,
                std::time::SystemTime::now()
            ));

            pending.insert(token.clone(), sender.clone());

            let msg = RpcMessage {
                token: token.clone(),
                src: self.addr.clone(),
                dst: addr,
                msg: Message::Request(req),
            };

            self.send_msg(&msg);

            let rpc = self.clone();
            thread::spawn(move || {
                thread::sleep(std::time::Duration::from_millis(TIMEOUT));
                if let Ok(_) = sender.send(None) {
                    let mut pending = rpc
                        .pending
                        .lock()
                        .expect("[FAILED] Rpc::make_request --> Failed to acquire mutex on Pending");
                    pending.remove(&token);
                }
            });

            rec


    }

}