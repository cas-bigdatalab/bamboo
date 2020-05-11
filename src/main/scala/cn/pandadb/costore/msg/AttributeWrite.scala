package cn.pandadb.costore.msg

case class AttributeWrite(msg: Map[String, String], vNodeID: Int)
