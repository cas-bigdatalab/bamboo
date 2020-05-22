package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing
class Config(val nodesInfo: List[String]) {
  val vNodeIDs = 0 until nodesInfo.length * globalConfig.vNodeNumberPerNode toList
  val vNodeID2NodeInfo = vNodeIDs.map(vid => (vid -> nodesInfo(vid % nodesInfo.length))).toMap
  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))
  def route(node: Map[String, String]): List[(Int, String)] = {
    vNodeRing.getHolders(node.get("id").get, globalConfig.replicaFactor).map(hid => {
      val id =  hid.toInt
      (id, vNodeID2NodeInfo.get(id).get)
    })
  }
}
