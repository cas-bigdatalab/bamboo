package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing

object globalConfig {
  val flushInterval = 1
  val replicaFactor = 3
  val vNodeNumberPerNode = 3//TODO config per node
  val nodesInfo = List("localhost:11234", "localhost:11235", "localhost:11236")
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
