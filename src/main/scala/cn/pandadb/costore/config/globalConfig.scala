package cn.pandadb.costore.config

import cn.pandadb.costore.{NodeRpc, Shard}

object globalConfig {
  val shards = List(new Shard(0), new Shard(1), new Shard(2))
  val nodes = List("localhost:11234", "localhost:11235", "localhost:11236")
  val shards2Nodes = Map(0 -> nodes(0), 1 -> nodes(1), 2 -> nodes(2))

  def route(node: Map[String, String]): (String, Int) = {
    val targetShardID = node.get("id").get.toInt%shards.length
    val targetNode = shards2Nodes.get(targetShardID).get
    (targetNode, targetShardID)
  }

}