package cn.pandadb.bamboo.rpc

import cn.pandadb.bamboo.rpc.msg._
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global

class NodeRpc(val address: String) {

  private val ipPort = address.split(':')
  val ip = ipPort(0)
  val port = ipPort(1).toInt
  private lazy val endPointRef = NettyRpcEnvFactory.create(
      RpcEnvClientConfig(new RpcConf(), "node-client")
    ).setupEndpointRef(
      new RpcAddress(ip, port),
      "node-service"
    )

  def filterNodes(kv: Map[String, String], vNodeID: String = "-1"): List[Map[String, String]] = {
    endPointRef.askWithRetry[List[Map[String, String]]](AttributeRead(kv, vNodeID))
  }
  // scalastyle:off
  def addNodeAsyn(docsToAdded: Map[String, String], vNodeID: String = "-1"): Future[String] = {
    val future = endPointRef.ask[String](AttributeWrite(docsToAdded, vNodeID))
    future.onComplete {
      case scala.util.Success(value) => { } // println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    future
  }
  def addNode(docsToAdded: Map[String, String], vNodeID: String): Unit = {
    endPointRef.askWithRetry[String](AttributeWrite(docsToAdded, vNodeID))
  }

  def deleteNode(docsToBeDeleted: Map[String, String], vNodeID: String = "-1"): Unit = {
    val future = endPointRef.ask[String](AttributeDelete(docsToBeDeleted, vNodeID))
    future.onComplete {
      case scala.util.Success(value) => println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteAll(vNodeID: String = "-1"): Unit = {
    val future = endPointRef.ask[String](AllDeleting(vNodeID))
    future.onComplete {
      case scala.util.Success(value) => println(s"$value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }
  // scalastyle:on
}