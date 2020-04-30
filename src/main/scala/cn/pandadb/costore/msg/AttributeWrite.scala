package cn.pandadb.costore.msg

case class AttributeWrite(msg: Map[String, String], shardID: Int)
