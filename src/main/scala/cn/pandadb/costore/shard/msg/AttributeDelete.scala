package cn.pandadb.costore.shard.msg

import cn.pandadb.costore.shard.Shard

case class AttributeDelete(msg: Map[String, String], shard: Shard)
