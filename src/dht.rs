use crate::node::{Message, Request, RequestType, Response, RpcMessage, ValueResult, NodeDistance};
use crate::routing::ChannelMsg;

use super::node::Node;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use super::routing;
use super::key;
use std::sync::mpsc;
use crossbeam_channel;

#[derive(Debug, Clone)]
pub struct DHT {
    pub routes: Arc<Mutex<routing::RoutingTable>>,
    pub store: Arc<Mutex<HashMap<String, String>>>,
    pub node: Node,

}

impl DHT {
    fn new(ip: String, port: u8, bootstrap: Option<Node>) -> Self {
        let node = Node::new(ip, port);
        let (rt_channel_sender, rt_channel_receiver) = crossbeam_channel::unbounded();
        let routes = routing::RoutingTable::new(
            node.clone(),
            bootstrap,
            rt_channel_sender.clone(),
            rt_channel_receiver.clone(),
        );

        let (rpc_channel_sender, rpc_channel_receiver) = mpsc::channel();
        Node::start(node.clone(), rpc_channel_sender);

        let dht = Self {
            routes: Arc::new(Mutex::new(routes)),
            store: Arc::new(Mutex::new(HashMap::new())),
            node: node.clone(),
        };

        dht.clone().requests_handler(rpc_channel_receiver);
        dht
            .clone()
            .rt_forwarder(rt_channel_sender, rt_channel_receiver);

        dht.nodes_lookup(&node.id);

        let protocol_clone = dht.clone();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_secs(60 * 60));
            protocol_clone.republish();
        });

        dht




    }

    fn republish(&self) {
        let st = self
            .store
            .lock()
            .expect("[FAILED] Protocol::republish --> Failed to acquire mutex on Store");
        for (key, value) in &*st {
            self.put(key.to_string(), value.to_string());
        }
    }

    pub fn put(&self, k: String, v: String) {
        let candidates = self.nodes_lookup(key::Key::new(k.clone()));

        for NodeDistance(node, _, _) in candidates {
            let protocol_clone = self.clone();
            let k_clone = k.clone();
            let v_clone = v.clone();

            std::thread::spawn(move || {
                protocol_clone.store(node, k_clone, v_clone);
            });
        }
    }

    pub fn store(&self, dst: Node, key: String, val: String) -> bool {
        let res =
            self.node.make_req(&self.rpc, network::Request::Store(key, val), dst.clone());

        // since we get a ping, update our routing table
        let mut routes = self
            .routes
            .lock()
            .expect("[FAILED] Protocol::store --> Failed to acquire mutex on Routes");
        if let Some(network::Response::Ping) = res {
            routes.update(dst);
            true
        } else {
            routes.remove(&dst);
            false
        }
    }

    pub fn ping(&self, dst: Node) -> bool {
        let res = self.node.make_req(RequestType::Ping, dst.clone().id, dst.addr)
        .recv()
        .expect("[FAILED] DTH::make_req --> Failed to receive response through channel");

        let mut routes = self
            .routes
            .lock()
            .expect("[FAILED] Protocol::ping --> Failed to acquire lock on Routes");

        if let Some(Response::Ping) = res {
            routes.update(dst.clone());
            true
        } else {
            eprintln!(
                "[WARNING] Protocol::Ping --> No response, removing contact from routing table"
            );
            routes.remove(&dst);
            false
        }
    }

    fn rt_forwarder(
        self,
        sender: crossbeam_channel::Sender<ChannelMsg>,
        receiver: crossbeam_channel::Receiver<ChannelMsg>,
    ) {
        std::thread::spawn(move || {
            for req in receiver.iter() {
                let protocol = self.clone();
                let sender_clone = sender.clone();

                std::thread::spawn(move || match req {
                    ChannelMsg::Request(payload) => match payload.0 {
                        RequestType::Ping => {
                            let success = protocol.ping(payload.1);
                            if success {
                                if let Err(_) = sender_clone
                                    .send(ChannelMsg::Response(Response::Ping))
                                {
                                    eprintln!("[FAILED] Protocol::rt_forwared --> Receiver is dead, closing channel");
                                }
                            } else {
                                if let Err(_) = sender_clone.send(ChannelMsg::NoData) {
                                    eprintln!("[FAILED] Protocol::rt_forwared --> Receiver is dead, closing channel");
                                }
                            }
                        }
                        _ => {
                            unimplemented!();
                        }
                    },
                    ChannelMsg::Response(_) => {
                        eprintln!("[FAILED] Protocol::rt_forwarder --> Received a Response instead of a Request")
                    }
                    ChannelMsg::NoData => {
                        eprintln!("[FAILED] Protocol::rt_forwarder --> Received a NoData instead of a Request")
                    }
                });
            }
        });
    }

    fn requests_handler(self, receiver: mpsc::Receiver<Request>) {
        std::thread::spawn(move || {
            for req in receiver.iter() {
                let dht = self.clone();

                std::thread::spawn(move || {
                    let res = dht.craft_res(req);
                    dht.reply(res);
                });
            }
        });
    }

    fn reply(&self, packet_details: (Response, Request)) {
        let msg = RpcMessage {
            token: packet_details.1.token,
            src: self.node.clone().addr,
            dst: packet_details.1.src,
            msg: Message::Response(packet_details.0),
        };

        self.node.send_msg(&msg);
    }

    pub fn find_node( &self, dst: super::key::Key, id: super::key::Key, addr: String) -> Option<Vec<NodeDistance>>{
            let res = self.node.make_req(RequestType::FindNode(id), dst.clone(), addr)
            .recv()
            .expect("[FAILED] DTH::make_req --> Failed to receive response through channel");

            let mut routes = self
                .routes
                .lock()
                .expect("[FAILED] Protocol::find_node --> Failed to acquire mutex on Routes");
            if let Some(Response::FindNode(entries)) = res {
                routes.update(dst);
                Some(entries)
            } else {
                routes.remove(&dst);
                None
            }
        }

    pub fn nodes_lookup(&self, id: key::Key) -> Vec<NodeDistance> {
        let mut ret: Vec<NodeDistance> = Vec::new();

        // nodes visited
        let mut queried = HashSet::new();
        let routes = self
            .routes
            .lock()
            .expect("[FAILED] Protocol::nodes_lookup --> Failed to acquire mutex on Routes");

        // nodes to visit
        let mut to_query = BinaryHeap::from(routes.closest_k(&id, 20));
        drop(routes);

        for entry in &to_query {
            queried.insert(entry.clone());
        }

        while !to_query.is_empty() {
            // threads joins
            let mut joins: Vec<std::thread::JoinHandle<Option<Vec<NodeDistance>>>> =
                Vec::new();
            // outgoing queries
            let mut queries: Vec<NodeDistance> = Vec::new();
            let mut results: Vec<Option<Vec<NodeDistance>>> = Vec::new();

            for _ in 0..3 {
                match to_query.pop() {
                    Some(entry) => {
                        queries.push(entry);
                    }
                    None => {
                        break;
                    }
                }
            }

            for &NodeDistance(ref node, _, addr) in &queries {
                let n = node.clone();
                let id_clone = id.clone();
                let protocol_clone = self.clone();

                joins.push(std::thread::spawn(move || {
                    protocol_clone.find_node(n, id_clone, addr)
                }));
            }

            for j in joins {
                results.push(j.join().expect(
                    "[FAILED] Protocol::nodes_lookup --> Failed to join thread while visiting nodes",
                ));
            }

            for (result, query) in results.into_iter().zip(queries) {
                if let Some(entries) = result {
                    ret.push(query);

                    for entry in entries {
                        if queried.insert(entry.clone()) {
                            to_query.push(entry);
                        }
                    }
                }
            }
        }

        ret.sort_by(|a, b| a.1.cmp(&b.1));
        ret.truncate(20);

        ret
    }

    fn craft_res(&self, req: Request) -> (Response, Request) {
        let mut routes = self
            .routes
            .lock()
            .expect("[FAILED] Protocol::craft_res --> Failed to acquire mutex on Routes");

        // must craft node object because ReqWrapper contains only the src string addr
        let split = req.src.split(":");
        let parsed: Vec<&str> = split.collect();

        let src_node = Node::new(
            parsed[0].to_string(),
            parsed[1]
                .parse::<u8>()
                .expect("[FAILED] Protocol::craft_res --> Failed to parse Node port from address"),
        );
        routes.update(src_node);
        drop(routes);

        match req.req {
            RequestType::Ping => (Response::Ping, req),
            RequestType::Store(ref k, ref v) => {
                // ref is used to borrow k and v, which are the contents of req

                let mut store = self
                    .store
                    .lock()
                    .expect("[FAILED] Protocol::craft_res --> Failed to acquire mutex on Store");
                store.insert(k.to_string(), v.to_string());

                (Response::Ping, req)
            }
            RequestType::FindNode(ref id) => {
                let routes = self
                    .routes
                    .lock()
                    .expect("[FAILED] Protocol::craft_res --> Failed to acquire mutex on Routes");

                let result = routes.closest_k(id, 20);

                (Response::FindNode(result), req)
            }
            RequestType::FindValue(ref k) => {
                let key = super::key::Key::new(k.to_string());
                let store = self
                    .store
                    .lock()
                    .expect("[FAILED] Protocol::craft_res --> Failed to acquire mutex on Store");

                let val = store.get(k);

                match val {
                    Some(v) => (
                        Response::FindNodeValue(ValueResult::Value(
                            v.to_string(),
                        )),
                        req,
                    ),
                    None => {
                        let routes = self.routes.lock().expect(
                            "[FAILED] Protocol::craft_res --> Failed to acquire mutex on Routes",
                        );
                        (
                            Response::FindNodeValue(ValueResult::Nodes(
                                routes.closest_k(&key, 20),
                            )),
                            req,
                        )
                    }
                }
            }
        }
    }
}