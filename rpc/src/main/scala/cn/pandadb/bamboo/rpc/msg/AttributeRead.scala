package cn.pandadb.bamboo.rpc.msg

case class AttributeRead(msg: Map[String, String], vNodeID: String)
