package cn.pandadb.costore.msg

case class AttributeRead(msg: Map[String, String], shardID: Int)
