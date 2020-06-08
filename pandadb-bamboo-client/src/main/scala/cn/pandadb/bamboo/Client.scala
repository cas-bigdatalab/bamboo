package cn.pandadb.bamboo

import cn.pandadb.bamboo.rpc.NodeRpc

import scala.concurrent.Future

class Client(val addresses: List[String], val balancePolicy: String = "RR", val balancePolicyBatch: Int = 100) {

  val coordinators = addresses.map(a => new NodeRpc(a))
  private var coordinatorCur: Int = -1
  private val rnd = new scala.util.Random
  private var requestCnt: Int = 0

  def getCoordinator(): NodeRpc = {
    if (requestCnt == 0) {
      coordinatorCur = balancePolicy match {
        case "FIRST" => 1
        case "RR" =>
          var v = coordinatorCur + 1
          if (v == addresses.length) {
            v = 0
          }
          v
        case "RND" => rnd.nextInt(addresses.length)
      }
    }
    requestCnt += 1
    if (requestCnt == balancePolicyBatch) {
      requestCnt = 0
    }
    coordinators(coordinatorCur)
  }

  def filterNodes(kv: Map[String, String]): List[Map[String, String]] = {
    getCoordinator.filterNodes(kv, "-1")
  }

  def addNodeAsyn(docsToAdded: Map[String, String]): Future[String] = {
    getCoordinator.addNodeAsyn(docsToAdded, "-1")
  }

  def addNode(docsToAdded: Map[String, String]): Unit = {
    getCoordinator.addNode(docsToAdded, "-1")
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    getCoordinator.deleteNode(docsToBeDeleted, "-1")
  }

  def deleteAll(): Unit = {
    getCoordinator.deleteAll("-1")
  }
}