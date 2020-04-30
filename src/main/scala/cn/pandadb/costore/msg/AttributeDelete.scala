package cn.pandadb.costore.msg

case class AttributeDelete(msg: Map[String, String], shardID: Int)
