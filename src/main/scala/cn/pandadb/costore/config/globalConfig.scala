package cn.pandadb.costore.config

import cn.pandadb.costore.shard.{NodeRPC, NodeService, Shard}

object globalConfig {
  val shards = Array(new Shard(0), new Shard(1), new Shard(2))
  val nodes = Array(new NodeRPC(new NodeService("localhost", 11234)), new NodeRPC(new NodeService("localhost", 11235)), new NodeRPC(new NodeService("localhost", 11236)))
  val shards2Nodes = shards.map(s => (s, nodes(s.id))).toMap
}