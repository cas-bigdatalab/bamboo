package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing
class Config(val nodesInfo: List[String], val replicaFactor: Int) {
  val vNodeIDs = 0 until nodesInfo.length * globalConfig.vNodeNumberPerNode toList
  val vNodeID2NodeInfo = vNodeIDs.map(vid => (vid -> nodesInfo(vid % nodesInfo.length))).toMap
  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))
  def getReplicaLocation(originLocation: String) = (0 until replicaFactor).toList.map(inc => {
    val beginIndex = nodesInfo.indexOf(originLocation)//TODO improve perf.
    nodesInfo((beginIndex+inc)%nodesInfo.length)
  })
  def route(node: Map[String, String]): List[(Int, String)] = {
    val primaryVNodeID = vNodeRing.getHolder(node.get("id").get, replicaFactor).toInt
    getReplicaLocation(vNodeID2NodeInfo.get(primaryVNodeID).get).map(l=>(primaryVNodeID, l))
  }
}
