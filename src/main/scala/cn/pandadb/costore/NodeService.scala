package cn.pandadb.costore

import java.util

import cn.pandadb.costore.config.globalConfig
import cn.pandadb.costore.msg.{AllDeleting, AttributeDelete, AttributeRead, AttributeWrite}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory

class NodeService(val address: String) {

  private val ipPort = address.split(':')
  val ip = ipPort(0)
  val port = ipPort(1).toInt
  private val config = RpcEnvServerConfig(new RpcConf(), "node-server", ip, port)
  private val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

  def start() = {
    rpcEnv.setupEndpoint("node-service", new NodeEndpoint(rpcEnv))
    rpcEnv.awaitTermination()
  }

  def stop() = {
    rpcEnv.shutdown()
  }

}

class NodeEndpoint(override val rpcEnv: RpcEnv) extends RpcEndpoint {

  private lazy val peerRpcs = globalConfig.nodes.map(address => (address -> new NodeRpc(address))).toMap

  override def onStart(): Unit = {
    println("start node endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AttributeWrite(msg, shardID) => {
      shardID match {
        case -1 => {
          val (node, shardID) = globalConfig.route(msg)
          peerRpcs.get(node).get.addNode(msg, shardID)
          context.reply(s"write $msg to shard $shardID on node $node")
        }
        case _ => {
          println(msg, shardID)
          globalConfig.shards(shardID).write(msg)
          context.reply(s"write $msg to local shard $shardID")
        }
      }
    }
    case AttributeRead(msg, shardID) =>{
      shardID match {
        case -1 => {
          val ret = new util.ArrayList[util.HashMap[String, String]]()
          globalConfig.shards2Nodes.foreach(shardNode => ret.addAll(peerRpcs.get(shardNode._2).get.filterNodes(msg, shardNode._1)))
          context.reply(ret)
        }
        case _ => {
          context.reply(globalConfig.shards(shardID).search(msg))
        }
      }
    }
    case AttributeDelete(msg, shardID) => {
      shardID match {
        case -1 => {
          val (node, shardID) = globalConfig.route(msg)
          peerRpcs.get(node).get.deleteNode(msg, shardID)
          context.reply(s"delete $msg from shard $shardID on node $node")
        }
        case _ => {
          globalConfig.shards(shardID).delete(msg)
          context.reply(s"delete $msg from local shard $shardID")
        }
      }
    }
    case AllDeleting(shardID) => {
      shardID match {
        case -1 => {
          globalConfig.shards2Nodes.foreach(shardNode => peerRpcs.get(shardNode._2).get.deleteAll(shardNode._1))
          context.reply(s"delete all from all shards")
        }
        case _ => {
          globalConfig.shards(shardID).deleteAll()
          context.reply(s"delete all from local shard $shardID")
        }
      }

    }
  }

  override def onStop(): Unit = {
    println("stop node endpoint")
  }
}