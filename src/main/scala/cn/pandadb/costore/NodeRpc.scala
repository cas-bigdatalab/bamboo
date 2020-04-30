package cn.pandadb.costore

import java.util

import cn.pandadb.costore.config.globalConfig
import cn.pandadb.costore.msg.{AllDeleting, AttributeDelete, AttributeRead, AttributeWrite}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

class NodeRpc(val ip: String, val port: Int) {

  val endPointRef = NettyRpcEnvFactory.create(
      RpcEnvClientConfig( new RpcConf(), "node-client")
    ).setupEndpointRef(
      new RpcAddress(ip, port),
      "node-service"
    )

  def filterNodes(kv: Map[String, String], shardID: Int = -1): util.ArrayList[util.HashMap[String, String]]  = {
    endPointRef.askWithRetry[util.ArrayList[util.HashMap[String, String]]](AttributeRead(kv, shardID))
  }

  def addNode(docsToAdded: Map[String, String], shardID: Int = -1): Unit = {
    if (shardID == -1){
      val (node, shard) = globalConfig.route(docsToAdded)
      return node.addNode(docsToAdded,shard)
    }
    val future = endPointRef.ask[String](AttributeWrite(docsToAdded, shardID))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteNode(docsToBeDeleted: Map[String, String], shardID: Int = -1): Unit = {
    if (shardID == -1){
      val (node, shard) = globalConfig.route(docsToBeDeleted)
      return node.deleteNode(docsToBeDeleted,shard)
    }
    val future = endPointRef.ask[String](AttributeDelete(docsToBeDeleted, shardID))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteAll(shardID: Int = -1): Unit = {
    if (shardID == -1){
      return globalConfig.shards2Nodes.foreach(shardNode => shardNode._2.deleteAll(shardNode._1))
    }
    val future = endPointRef.ask[String](AllDeleting(shardID))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }
}