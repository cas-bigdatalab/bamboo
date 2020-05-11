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

  private lazy val peerRpcs = globalConfig.nodesInfo.map(address => (address -> new NodeRpc(address))).toMap
  private lazy val vNodes = globalConfig.vNodeID2NodeInfo.filter(
    vNodeIDNodeInfo => vNodeIDNodeInfo._2 == rpcEnv.address.hostPort
  ).map(vNodeIDNodeInfo => (vNodeIDNodeInfo._1 -> new VNode(vNodeIDNodeInfo._1)))

  override def onStart(): Unit = {
    println("start node endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AttributeWrite(msg, vNodeID) => {
      vNodeID match {
        case -1 => {
          globalConfig.route(msg).map(vNodeIDNodeInfo => {
            val rpc = peerRpcs.get(vNodeIDNodeInfo._2).get
            rpc.addNode(msg, vNodeIDNodeInfo._1)
          })
          context.reply(s"coordinator ${rpcEnv.address}: writing $msg")
        }
        case _ => {
          vNodes.get(vNodeID).get.write(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg written")
        }
      }
    }
    case AttributeRead(msg, vNodeID) =>{
      vNodeID match {
        case -1 => {
          val ret = new util.ArrayList[util.HashMap[String, String]]()
          globalConfig.vNodeID2NodeInfo.foreach(vNodeNode =>
            ret.addAll(peerRpcs.get(vNodeNode._2).get.filterNodes(msg, vNodeNode._1))
          )
          context.reply(ret)
        }
        case _ => {
          context.reply(vNodes.get(vNodeID).get.search(msg))
        }
      }
    }
    case AttributeDelete(msg, vNodeID) => {
      vNodeID match {
        case -1 => {
          globalConfig.route(msg).map(vNodeIDNodeInfo => {
            val rpc = peerRpcs.get(vNodeIDNodeInfo._2).get
            rpc.deleteNode(msg, vNodeIDNodeInfo._1)
          })
          context.reply(s"coordinator ${rpcEnv.address}: deleting $msg")
        }
        case _ => {
          vNodes.get(vNodeID).get.delete(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg deleted")
        }
      }
    }
    case AllDeleting(vNodeID) => {
      vNodeID match {
        case -1 => {
          globalConfig.vNodeID2NodeInfo.foreach(vNodeNode =>
            peerRpcs.get(vNodeNode._2).get.deleteAll(vNodeNode._1)
          )
          context.reply(s"coordinator ${rpcEnv.address}: deleting all from all vNodes")
        }
        case _ => {
          vNodes.get(vNodeID).get.deleteAll()
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: all deleted")
        }
      }

    }
  }

  override def onStop(): Unit = {
    println("stop node endpoint")
  }
}