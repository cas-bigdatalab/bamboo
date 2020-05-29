package cn.pandadb.costore.msg

case class AttributeWriteAsyn(msg: Map[String, String], vNodeID: Int)
