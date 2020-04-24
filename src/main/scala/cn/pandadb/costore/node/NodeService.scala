package cn.pandadb.costore.node

import msg._
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class NodeService(ip: String, port: Int) {

  var shards: Array[ShardService] = null

  val config = RpcEnvServerConfig(new RpcConf(), "node-server", ip, port)
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

  def start() = {
    rpcEnv.setupEndpoint("node-service", new NodeEndpoint(rpcEnv))
    rpcEnv.awaitTermination()
  }

  def stop() = {
    rpcEnv.shutdown()
  }

}

class NodeEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  override def onStart(): Unit = {
    println("start node endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AttributeWrite(msg) => {
//      val shard = Len(shards)
      Indices.write(msg)
      context.reply(s"write $msg")
    }
    case AttributeRead(msg) => {
      context.reply(Indices.search(msg))
    }
    case AttributeDelete(msg) => {
      Indices.delete(msg)
      context.reply(s"delete $msg")
    }
    case AllDeleting() => {
      Indices.deleteAll()
      context.reply(s"delete all")
    }
  }

  override def onStop(): Unit = {
    println("stop node endpoint")
  }
}