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

class NodeRpc(val address: String) {

  private val ipPort = address.split(':')
  val ip = ipPort(0)
  val port = ipPort(1).toInt
  private lazy val endPointRef = NettyRpcEnvFactory.create(
      RpcEnvClientConfig( new RpcConf(), "node-client")
    ).setupEndpointRef(
      new RpcAddress(ip, port),
      "node-service"
    )

  def filterNodes(kv: Map[String, String], vNodeID: Int = -1): util.ArrayList[util.HashMap[String, String]]  = {
    endPointRef.askWithRetry[util.ArrayList[util.HashMap[String, String]]](AttributeRead(kv, vNodeID))
  }

  def addNode(docsToAdded: Map[String, String], vNodeID: Int = -1): Unit = {
    val future = endPointRef.ask[String](AttributeWrite(docsToAdded, vNodeID))
    future.onComplete {
      case scala.util.Success(value) => {}// println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def addNodeWithRetry(docsToAdded: Map[String, String], vNodeID: Int): Unit = {
    endPointRef.askWithRetry[util.ArrayList[util.HashMap[String, String]]](AttributeWrite(docsToAdded, vNodeID))
  }

  def deleteNode(docsToBeDeleted: Map[String, String], vNodeID: Int = -1): Unit = {
    val future = endPointRef.ask[String](AttributeDelete(docsToBeDeleted, vNodeID))
    future.onComplete {
      case scala.util.Success(value) => println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteAll(vNodeID: Int = -1): Unit = {
    val future = endPointRef.ask[String](AllDeleting(vNodeID))
    future.onComplete {
      case scala.util.Success(value) => println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }
}