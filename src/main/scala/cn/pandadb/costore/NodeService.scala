package cn.pandadb.costore

import java.util

import cn.pandadb.costore.config.Config
import cn.pandadb.costore.msg.{AllDeleting, AttributeDelete, AttributeRead, AttributeWriteAsyn, AttributeWriteSyn}
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc._
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory


class NodeService(val address: String, val peers: List[String], val replicaFactor: Int) {

  private val ipPort = address.split(':')
  val ip = ipPort(0)
  val port = ipPort(1).toInt
  private val config = RpcEnvServerConfig(new RpcConf(), "node-server", ip, port)
  private val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

  def start() = {
    rpcEnv.setupEndpoint("node-service", new NodeEndpoint(rpcEnv, peers, replicaFactor))
    rpcEnv.awaitTermination()
  }

  def stop() = {
    rpcEnv.shutdown()
  }

}

class NodeEndpoint(override val rpcEnv: RpcEnv, val peers: List[String], val replicaFactor: Int) extends RpcEndpoint {

  private val config = new Config(peers, replicaFactor)
  private lazy val peerRpcs = config.nodesInfo.map(address => (address -> new NodeRpc(address))).toMap
  private lazy val vNodes = config.getVNodeByNodeInfo(rpcEnv.address.hostPort).map(vid => (vid -> new VNode(vid))).toMap
  println(s"${rpcEnv.address.hostPort} hold vNodes: $vNodes")

  override def onStart(): Unit = {
    println("start node endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AttributeWriteSyn(msg, vNodeID) => {
      vNodeID match {
        case "-1" => {
          val targetVNodeIDNodeInfos = config.route(msg)
          val (primaryVNodeID, primaryNodeInfo) = targetVNodeIDNodeInfos.head
          peerRpcs.get(primaryNodeInfo).get.addNodeWithRetry(msg, primaryVNodeID)
          targetVNodeIDNodeInfos.tail.par.foreach(vNodeIDNodeInfo => {
            peerRpcs.get(vNodeIDNodeInfo._2).get.addNode(msg, vNodeIDNodeInfo._1)
          })
          context.reply(s"coordinator ${rpcEnv.address}: writing $msg")
        }
        case _ => {
          vNodes.get(s"$vNodeID").get.write(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg written")
        }
      }
    }
    case AttributeWriteAsyn(msg, vNodeID) => {
      vNodeID match {
        case "-1"=> {
          val targetVNodeIDNodeInfos = config.route(msg)
          val (primaryVNodeID, primaryNodeInfo) = targetVNodeIDNodeInfos.head
          peerRpcs.get(primaryNodeInfo).get.addNode(msg, primaryVNodeID)
          targetVNodeIDNodeInfos.tail.par.foreach(vNodeIDNodeInfo => {
            peerRpcs.get(vNodeIDNodeInfo._2).get.addNode(msg, vNodeIDNodeInfo._1)
          })
          context.reply(s"coordinator ${rpcEnv.address}: writing $msg")
        }
        case _ => {
//          println(s"${rpcEnv.address.hostPort} write vNode: $vNodeID")
          vNodes.get(vNodeID).get.write(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg written")
        }
      }
    }
    case AttributeRead(msg, vNodeID) =>{//TODO: change read  from failed main  replica  to choose replica
      vNodeID match {
        case "-1" => {
          val ret = new util.ArrayList[util.HashMap[String, String]]()
          config.vNodeID2NodeInfos.par.foreach(vNodeNodes =>
            ret.addAll(peerRpcs.get(vNodeNodes._2.head._2).get.filterNodes(msg, vNodeNodes._2.head._1))
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
        case "-1" => {
          config.route(msg).foreach(vNodeIDNodeInfo => {
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
        case "-1" => {
          config.vNodeID2NodeInfos.foreach(vNodeNodes =>
            vNodeNodes._2.foreach(node => {
              peerRpcs.get(node._2).get.deleteAll(node._1)
            })
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
