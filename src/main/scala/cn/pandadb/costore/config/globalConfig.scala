package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing
import java.util.Properties

object globalConfig {
//  lazy val props = new Properties()
//  props.load(globalConfig.getClass.getResourceAsStream("../config/config.properties"))
  val nodesInfo =  List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234")//props.getProperty("nodesInfo").split(",")
  val replicaFactor = 3//props.getProperty("replicaFactor").toInt
  val vNodeNumberPerNode = 3//props.getProperty("vNodeNumberPerNode").toInt
  val flushInterval = 1//props.getProperty("flushInterval").toInt
  val vNodeIDs = 0 until nodesInfo.length * vNodeNumberPerNode toList
  val vNodeID2NodeInfo = vNodeIDs.map(vid => (vid -> nodesInfo(vid % nodesInfo.length))).toMap
  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))

  def route(node: Map[String, String]): List[(Int, String)] = {
    vNodeRing.getHolders(node.get("id").get, replicaFactor).map(hid => {
      val id =  hid.toInt
      (id, vNodeID2NodeInfo.get(id).get)
    })
  }
}
