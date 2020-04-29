package cn.pandadb.costore.shard.msg

import cn.pandadb.costore.shard.Shard

case class AttributeRead(msg: Map[String, String], shard: Shard)
