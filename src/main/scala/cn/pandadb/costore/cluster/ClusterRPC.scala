package cn.pandadb.costore.cluster

import java.util

import cn.pandadb.costore.node.NodeRPC
import cn.pandadb.costore.node.msg.{AllDeleting, AttributeDelete, AttributeRead, AttributeWrite}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}

import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

class ClusterRPC(ip: String, port: Int) extends NodeRPC (ip: String, port: Int) { //TODO: hippo rpc
  val leaderNode = this
  val nodesIPRing = Array(("localhost", 11234), ("localhost", 11235), ("localhost", 11236)) //new mutable.HashMap[String, Int]()
  val liveNodes = nodesIPRing.map(ipPort => new NodeRPC(ipPort._1, ipPort._2))

  def route(node: Map[String, String]): NodeRPC = {
    liveNodes(node.get("id").get.toInt%liveNodes.length)
  }

  override def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    val ret = new util.ArrayList[util.HashMap[String, String]]()
    liveNodes.foreach( n => {
      ret.addAll(n.filterNodes(kv))
    })
    ret
  }

  override def addNode(docsToAdded: Map[String, String]): Unit = {
    route(docsToAdded).addNode(docsToAdded)
  }

  override def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    route(docsToBeDeleted).deleteNode(docsToBeDeleted)
  }

  override def deleteAll(): Unit = {
    liveNodes.foreach( n => n.deleteAll())
  }
}