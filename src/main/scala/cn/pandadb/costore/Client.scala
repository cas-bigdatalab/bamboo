package cn.pandadb.costore

import java.util

class Client(val coordinator: NodeRpc) { //TODO: hippo rpc

  def filterNodes(kv: Map[String, String]): util.ArrayList[util.HashMap[String, String]]  = {
    coordinator.filterNodes(kv, -1)
  }

  def addNode(docsToAdded: Map[String, String]): Unit = {
    coordinator.addNode(docsToAdded, -1)
  }

  def deleteNode(docsToBeDeleted: Map[String, String]): Unit = {
    coordinator.deleteNode(docsToBeDeleted, -1)
  }

  def deleteAll(): Unit = {
    coordinator.deleteAll(-1)
  }

}