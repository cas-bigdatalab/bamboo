package cn.pandadb.costore

import java.util

class Client(val addresses: List[String], val balancePolicy: String = "RR", val balancePolicyBatch: Int = 100) { //TODO: hippo rpc

  val coordinators = addresses.map(a => new NodeRpc(a))
  private var coordinatorCur: Int = -1
  private val rnd = new scala.util.Random
  private var requestCnt: Int = 0

  def getCoordinator(): NodeRpc ={
    if (requestCnt == 0) {
      coordinatorCur = balancePolicy match {
        case "FIRST" => 1
        case "RR" => {
          var v = coordinatorCur + 1
          if (v == addresses.length) {
            v = 0
          }
          v
        }
        case "RND" => rnd.nextInt(addresses.length)
      }
    }
    requestCnt += 1
    if (requestCnt == balancePolicyBatch){
      requestCnt = 0
    }
    coordinators(coordinatorCur)
  }

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    println(s"search $kv:")
    getCoordinator.filterNodes(kv, "-1")
  }

  def addNodeSyn(docsToAdded: Map[String, String]): Unit = {
    getCoordinator.addNodeWithRetry(docsToAdded, "-1")
  }

  def addNodeAsyn(docsToAdded: Map[String, String]): Unit = {
    getCoordinator.addNode(docsToAdded, "-1")
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    getCoordinator.deleteNode(docsToBeDeleted, "-1")
  }

  def deleteAll(): Unit = {
    getCoordinator.deleteAll("-1")
  }
}