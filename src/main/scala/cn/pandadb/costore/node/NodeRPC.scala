package cn.pandadb.costore.node

import java.util

import cn.pandadb.costore.node.msg._
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.{RpcAddress, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Await
import scala.concurrent.duration.Duration

class NodeRPC(ip: String, port: Int) {
  val rpcConf = new RpcConf()
  val config = RpcEnvClientConfig(rpcConf, "node-client")
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  val endPointRef: RpcEndpointRef = rpcEnv.setupEndpointRef(RpcAddress(ip, port), "node-service")

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    endPointRef.askWithRetry[util.ArrayList[util.HashMap[String, String]]](AttributeRead(kv))
  }

  def addNode(docsToAdded: Map[String, String]): Unit = {
    val future = endPointRef.ask[String](AttributeWrite(docsToAdded))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    val future = endPointRef.ask[String](AttributeDelete(docsToBeDeleted))
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }

  def deleteAll(): Unit = {
    val future = endPointRef.ask[String](AllDeleting())
    future.onComplete {
      case scala.util.Success(value) => println(s"Got the result = $value")
      case scala.util.Failure(e) => println(s"Got error: $e")
    }
    Await.result(future, Duration.apply("30s"))
  }
}
