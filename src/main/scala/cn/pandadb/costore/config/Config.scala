package cn.pandadb.costore.config

import cn.pandadb.costore.utils.ConsistentHashRing

class Config(val nodesInfo: List[String], val replicaFactor: Int) {
  val vNodeIDs = 0 until nodesInfo.length * globalConfig.vNodeNumberPerNode toList
  lazy val vNodeID2NodeInfos = vNodeIDs.map(vid => {
    val beginIndex = vid % nodesInfo.length
    (vid -> (0 until replicaFactor).toList.map(inc => {
      nodesInfo((beginIndex+inc)%nodesInfo.length)
    }))
  }).toMap

  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))

  def route(doc: Map[String, String]): List[(Int, String)] = {
    val primaryVNodeID = vNodeRing.getHolder(doc.get("id").get).toInt
    vNodeID2NodeInfos.get(primaryVNodeID).get.map(NodeInfo=>(primaryVNodeID, NodeInfo))
  }
}
