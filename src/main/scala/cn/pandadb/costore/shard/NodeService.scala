package cn.pandadb.costore.shard

import msg._
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

class NodeService(ip: String, port: Int) {

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
    case AttributeWrite(msg, shard: Int) => {
      new Shard(shard).write(msg)
      context.reply(s"write $msg")
    }
    case AttributeRead(msg, shard: Int) => {
      context.reply(new Shard(shard).search(msg))
    }
    case AttributeDelete(msg, shard: Int) => {
      new Shard(shard).delete(msg)
      context.reply(s"delete $msg")
    }
    case AllDeleting(shard: Int) => {
      new Shard(shard).deleteAll()
      context.reply(s"delete all")
    }
  }

  override def onStop(): Unit = {
    println("stop node endpoint")
  }
}