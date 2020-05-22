package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing
class Config(val nodesInfo: List[String]) {
  val vNodeIDs = 0 until nodesInfo.length * globalConfig.vNodeNumberPerNode toList
  val vNodeID2NodeInfo = vNodeIDs.map(vid => (vid -> nodesInfo(vid % nodesInfo.length))).toMap
  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))
  def route(node: Map[String, String], replicaFactor: Int): List[(Int, String)] = {
    val primaryVNodeID = vNodeRing.getHolder(node.get("id").get, replicaFactor).toInt
    val beginIndex = nodesInfo.indexOf(vNodeID2NodeInfo.get(primaryVNodeID).get)
    (0 until replicaFactor).toList.map(inc => (primaryVNodeID, nodesInfo(beginIndex+inc)))
  }
}
