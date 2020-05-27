package cn.pandadb.costore

import java.util

class Client(val addresses: List[String], val balancePolicy: String = "FIRST", val balancePolicyBatch: Int = 100) { //TODO: hippo rpc

  val coordinators = addresses.map(a => new NodeRpc(a))
  private var coordinatorCur: Int = -1
  private val rnd = new scala.util.Random
  private val requestCnt = 0

  def getCoordinator(): NodeRpc ={
    coordinatorCur+=1
    if (requestCnt%balancePolicyBatch == 0) {
      coordinatorCur = balancePolicy match {
        case "FIRST" => 1
        case "RR" => (coordinatorCur + 1) % addresses.length
        case "RND" => rnd.nextInt(addresses.length)
      }
    }
    coordinators(coordinatorCur)
  }

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    println(s"search $kv:")
    getCoordinator.filterNodes(kv, -1)
  }

  def addNode(docsToAdded: Map[String, String]): Unit = {
    getCoordinator.addNode(docsToAdded, -1)
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    getCoordinator.deleteNode(docsToBeDeleted, -1)
  }

  def deleteAll(): Unit = {
    getCoordinator.deleteAll(-1)
  }
}