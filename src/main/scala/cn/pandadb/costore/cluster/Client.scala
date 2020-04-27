package cn.pandadb.costore.cluster

import java.util

import cn.pandadb.costore.node.NodeRPC
import cn.pandadb.costore.node.msg._
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import org.apache.lucene.document.Document

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class Client(leader: NodeRPC){//(nodes: Array[String]) {

//  val cluster = new ClusterRPC(nodes)
//
  var nodeRPC: NodeRPC = leader//cluster.leaderNode

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    nodeRPC.filterNodes(kv)
  }

  def addNode(kv: Map[String, String]): Unit  = {
    nodeRPC.addNode(kv)
  }

  def deleteNode(kv: Map[String, String]): Unit  = {
    nodeRPC.deleteNode(kv)
  }

  def deleteAll(): Unit  = {
    nodeRPC.deleteAll()
  }

}
