package cn.pandadb.bamboo.server

import cn.pandadb.bamboo.rpc.NodeRpc
import cn.pandadb.bamboo.rpc.msg._
import cn.pandadb.bamboo.server.config.Config
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcCallContext, RpcEndpoint, RpcEnv, RpcEnvServerConfig}

import scala.collection.mutable
import org.apache.logging.log4j.scala.Logging

class NodeService (val address: String, val peers: List[String], val replicaFactor: Int) {

  private val ipPort = address.split(':')
  val ip = ipPort(0)
  val port = ipPort(1).toInt
  private val config = RpcEnvServerConfig(new RpcConf(), "node-server", ip, port)
  private val rpcEnv: RpcEnv = NettyRpcEnvFactory.create(config)

  def start(): Unit = {
    rpcEnv.setupEndpoint("node-service", new NodeEndpoint(rpcEnv, peers, replicaFactor))
    rpcEnv.awaitTermination()
  }

  def stop(): Unit = {
    rpcEnv.shutdown()
  }

}

class NodeEndpoint(override val rpcEnv: RpcEnv, val peers: List[String], val replicaFactor: Int) extends RpcEndpoint with Logging {

  private val config = new Config(peers, replicaFactor)
  private lazy val peerRpcs = config.nodesInfo.map(address => (address -> new NodeRpc(address))).toMap
  private lazy val vNodes = config.getVNodeByNodeInfo(rpcEnv.address.hostPort).map(vid => (vid -> new VNode(vid))).toMap

  logger.info(s"${rpcEnv.address.hostPort} hold vNodes: $vNodes")

  override def onStart(): Unit = {
    logger.info("start node endpoint")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case AttributeWrite(msg, vNodeID) =>
      vNodeID match {
        case "-1" =>
          val targetVNodeIDNodeInfos = config.route(msg)
          val (primaryVNodeID, primaryNodeInfo) = targetVNodeIDNodeInfos.head
          targetVNodeIDNodeInfos.tail.par.foreach(vNodeIDNodeInfo => {
            peerRpcs.get(vNodeIDNodeInfo._2).get.addNodeAsyn(msg, vNodeIDNodeInfo._1)
          })
          peerRpcs.get(primaryNodeInfo).get.addNode(msg, primaryVNodeID)
          context.reply(s"coordinator ${rpcEnv.address}: writing $msg")
        case _ =>
          vNodes.get(s"$vNodeID").get.write(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg written")
      }
    case AttributeRead(msg, vNodeID) => //TODO: change read  from failed main  replica  to choose replica
      vNodeID match {
        case "-1" =>
          val ret = mutable.ListBuffer[Map[String, String]]()
          config.vNodeID2NodeInfos.par.foreach(vNodeNodes =>
            ret ++= (peerRpcs.get(vNodeNodes._2.head._2).get.filterNodes(msg, vNodeNodes._2.head._1))
          )
          context.reply(ret.toList)
        case _ =>
          context.reply(vNodes.get(vNodeID).get.search(msg))
      }
    case AttributeDelete(msg, vNodeID) =>
      vNodeID match {
        case "-1" =>
          config.route(msg).foreach(vNodeIDNodeInfo => {
            val rpc = peerRpcs.get(vNodeIDNodeInfo._2).get
            rpc.deleteNode(msg, vNodeIDNodeInfo._1)
          })
          context.reply(s"coordinator ${rpcEnv.address}: deleting $msg")
        case _ =>
          vNodes.get(vNodeID).get.delete(msg)
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: $msg deleted")
      }
    case AllDeleting(vNodeID) =>
      vNodeID match {
        case "-1" =>
          config.vNodeID2NodeInfos.foreach(vNodeNodes =>
            vNodeNodes._2.foreach(node => {
              peerRpcs.get(node._2).get.deleteAll(node._1)
            })
          )
          context.reply(s"coordinator ${rpcEnv.address}: deleting all from all vNodes")
        case _ =>
          vNodes.get(vNodeID).get.deleteAll()
          context.reply(s"vNode $vNodeID on ${rpcEnv.address}: all deleted")
      }
  }

  override def onStop(): Unit = {
    logger.info("stop node endpoint")
  }

}
