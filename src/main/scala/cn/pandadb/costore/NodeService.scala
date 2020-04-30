package cn.pandadb.costore

import java.util

import cn.pandadb.costore.config.globalConfig
import cn.pandadb.costore.msg.{AllDeleting, AttributeDelete, AttributeRead, AttributeWrite}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

class NodeService(val ip: String, val port: Int) {

  val config = RpcEnvServerConfig(new RpcConf(), "node-server", ip, port)
  val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)
  var endPointRef: RpcEndpointRef = null

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
    case AttributeWrite(msg, shardID) => {
      println(msg, shardID)
      globalConfig.shards(shardID).write(msg)
      context.reply(s"write $msg")
    }
    case AttributeRead(msg, shardID) => {
      if (shardID == -1) {
        val ret = new util.ArrayList[util.HashMap[String, String]]()
        globalConfig.shards2Nodes.foreach(shardNode => ret.addAll(shardNode._2.filterNodes(kv, shardNode._1)))
        return context.reply(ret)
      }
      context.reply(globalConfig.shards(shardID).search(msg))
    }
    case AttributeDelete(msg, shardID) => {
      globalConfig.shards(shardID).delete(msg)
      context.reply(s"delete $msg")
    }
    case AllDeleting(shardID) => {
      globalConfig.shards(shardID).deleteAll()
      context.reply(s"delete all")
    }
  }

  override def onStop(): Unit = {
    println("stop node endpoint")
  }
}