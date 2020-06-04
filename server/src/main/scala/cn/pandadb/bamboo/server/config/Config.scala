package cn.pandadb.bamboo.server.config

import cn.pandadb.bamboo.server.utils.ConsistentHashRing

import scala.collection.mutable

class Config(val nodesInfo: List[String], val replicaFactor: Int) {
  val vNodeIDs = 0 until nodesInfo.length * globalConfig.vNodeNumberPerNode toList
  lazy val vNodeID2NodeInfos = vNodeIDs.map(vid => (vid -> {
    val beginIndex = vid % nodesInfo.length
    (0 until replicaFactor).toList.map(inc => (s"${vid}_replica_$inc", nodesInfo((beginIndex + inc) % nodesInfo.length)))
  })).toMap

  val vNodeRing = new ConsistentHashRing(vNodeIDs.map(id => id.toString))

  def getVNodeByNodeInfo(nodeInfo: String): List[String] = {
    val vs = mutable.ListBuffer[String]()
    vNodeID2NodeInfos.foreach(vidN => {
      vidN._2.foreach(vn => {
        if (vn._2 == nodeInfo) {
          vs += vn._1
        }
      })
    })
    vs.toList
  }

  def route(doc: Map[String, String]): List[(String, String)] = {
    vNodeID2NodeInfos.get(vNodeRing.getHolder(doc.get("id").get).toInt).get
  }
}
