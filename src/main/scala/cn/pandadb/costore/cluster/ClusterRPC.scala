package cn.pandadb.costore.cluster

import java.util

import cn.pandadb.costore.config.globalConfig
import cn.pandadb.costore.shard.{NodeRPC, Shard}

class ClusterRPC(ip: String, port: Int) { //TODO: hippo rpc

  private def route(node: Map[String, String]): (NodeRPC, Shard) = {
    val targetShard = globalConfig.shards(node.get("id").get.toInt%globalConfig.shards.length)
    val targetNode = globalConfig.shards2Nodes.get(targetShard).get
    (targetNode, targetShard)
  }

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    val ret = new util.ArrayList[util.HashMap[String, String]]()
    globalConfig.shards2Nodes.foreach(shardNode => ret.addAll(shardNode._2.filterNodes(kv,  shardNode._1)))
    ret
  }

  def addNode(docsToAdded: Map[String, String]): Unit = {
    val (node, shard) = route(docsToAdded)
    node.addNode(docsToAdded,shard)
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    val (node, shard) = route(docsToBeDeleted)
    node.deleteNode(docsToBeDeleted, shard)
  }

  def deleteAll(): Unit = {
    globalConfig.shards2Nodes.foreach(shardNode => shardNode._2.deleteAll(shardNode._1))
  }
}