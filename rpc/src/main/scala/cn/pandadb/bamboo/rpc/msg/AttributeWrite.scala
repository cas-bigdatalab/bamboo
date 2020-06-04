package cn.pandadb.bamboo.rpc.msg

case class AttributeWrite(msg: Map[String, String], vNodeID: String)
