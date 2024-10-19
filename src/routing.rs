use std::process::id;

use crate::key::{Key, KEY_LEN};

use super::node::*;
use crossbeam_channel;


const N_BUCKETS: usize = KEY_LEN * 8;
const K_PARAM: usize = 20;


#[derive(Debug)]
pub struct KBucket {
    pub nodes: Vec<Node>,
    pub size: usize,
}

pub enum ChannelMsg {
    Request((RequestType, Node)),
    Response(Response),
    NoData,
}

#[derive(Debug)]
pub struct RoutingTable {
    pub node: Node,
    pub kbuckets: Vec<KBucket>,
    pub sender: crossbeam_channel::Sender<ChannelMsg>,
    pub receiver: crossbeam_channel::Receiver<ChannelMsg>,
}


impl KBucket {
    pub fn new() -> Self {
        KBucket {
            nodes: Vec::new(),
            size: 20,
        }
    }
}

impl RoutingTable {
    pub fn new(
        node: Node,
        bootstrap: Option<Node>,
        sender: crossbeam_channel::Sender<ChannelMsg>,
        receiver: crossbeam_channel::Receiver<ChannelMsg>,
    ) -> Self {
        let mut kbuckets: Vec<KBucket> = Vec::new();
        for _ in 0..N_BUCKETS {
            kbuckets.push(KBucket::new());
        }

        let mut ret = Self {
            node: node.clone(),
            kbuckets,
            sender,
            receiver,
        };

        ret.update(node);

        if let Some(bootstrap) = bootstrap {
            ret.update(bootstrap);
        }

        ret


    }

    pub fn get_lookup_idx(&self, key: &Key) -> usize {
        let d = Distance::new(&self.node.id, key);
        
        for i in 0..KEY_LEN {
            for j in (0..8).rev() {
                if (d.0[i] >> (7 - j)) & 0x1 != 0 {
                    return i * 8 + j;
                }
            }
        }

        KEY_LEN * 8 - 1
    }

    fn contact_via_rpc(&self, dst: Node) -> bool {
        if let Err(_) = self
            .sender
            .send(ChannelMsg::Request((RequestType::Ping, dst)))
        {
            println!(
                "[FAILED] RoutingTable::contact_via_rpc --> Receiver is dead, closing channel"
            );
            return false;
        }

        true
    }

    pub fn update(&mut self, node: Node) {
        // get index of kbuckets 
        let idx = self.get_lookup_idx(&node.id);

        if self.kbuckets[idx].nodes.len() < K_PARAM {
            let node_idx = self.kbuckets[idx].nodes.iter().position(|x| x.id == node.id);

            match node_idx {
                Some(i) => {
                    self.kbuckets[idx].nodes.remove(i);
                    self.kbuckets[idx].nodes.push(node);
                }
                None => {
                    self.kbuckets[idx].nodes.push(node);
                }
            }
        } else {
            let _success = self.contact_via_rpc(self.kbuckets[idx].nodes[0].clone());
            let receiver = self.receiver.clone();
            let res = receiver
                .recv()
                .expect("[FAILED] Routing::update --> Failed  to receive data from channel");
            match res {
                ChannelMsg::Response(_) => {
                    let to_re_add = self.kbuckets[idx].nodes.remove(0);
                    self.kbuckets[idx].nodes.push(to_re_add);
                }
                ChannelMsg::Request(_) => {
                    eprintln!("[FAILED] Routing::update --> Unexpectedly got a Request instead of a Response");
                }
                ChannelMsg::NoData => {
                    self.kbuckets[idx].nodes.remove(0);
                    self.kbuckets[idx].nodes.push(node);
                }
            };


        }
        //
    }

    pub fn remove(&mut self, node: &Node) {
        let idx = self.get_lookup_idx(&node.id);

        if let Some(i) = self.kbuckets[idx].nodes.iter().position(|x| x.id == node.id) {
            self.kbuckets[idx].nodes.remove(i);
        } else {
            eprintln!("[WARN] Routing::remove --> Tried to remove non-existing entry");
        }

    }

    pub fn closest_k(&self, key: &Key, count: usize) -> Vec<NodeDistance> {
        let mut ret = Vec::with_capacity(count);

        if count == 0 {
            return ret;
        }

        let mut idx = self.get_lookup_idx(key);
        for node in &self.kbuckets[idx].nodes {
            ret.push(NodeDistance(node.clone().id, Distance::new(&node.id, key), node.clone().addr));
        }

        let mut idx_copy = idx;

        while ret.len() < count && idx < self.kbuckets.len() - 1 {
            idx += 1;
            for node in &self.kbuckets[idx].nodes {
                ret.push(NodeDistance(node.clone().id, Distance::new(&node.id, key), node.clone().addr));
            }
        }

        while ret.len() < count && idx_copy > 0 {
            idx_copy -= 1;
            for node in &self.kbuckets[idx_copy].nodes {
                ret.push(NodeDistance(node.clone().id, Distance::new(&node.id, key), node.clone().addr));
            }
        }

        ret.sort_by(|a, b| a.1.cmp(&b.1));
        ret.truncate(count);

        ret
    }
}


