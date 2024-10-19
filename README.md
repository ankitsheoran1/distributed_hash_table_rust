# distributed_hash_table_rust

Key 
  [] 32 * 8 

Distance(k1, k2) 
   0...32 
     k1[i] ^k2[i]

bucket 
 [] Nodes

 Buckets -
   [] bucket - 256 

 findANodeInWhichBucket(node id)
     find set bit of id.key  and it should lie in that bucket 

 closestK(node id)
     Node f that bucket 


Node {
    key 
    ip
    port
    pending

}


RoutingTable {
    Node 
    Buckets
    listener 
    producer 
}



dht {
  routes
  stores
  node
}


## One (producer, consumer for ping pong and state update)

##Another (producer consumer) for req served

