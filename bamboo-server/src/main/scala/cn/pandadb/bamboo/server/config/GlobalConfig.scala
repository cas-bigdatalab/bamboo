package cn.pandadb.bamboo.server.config

object GlobalConfig {
  //  lazy val props = new Properties()
  //  props.load(globalConfig.getClass.getResourceAsStream("../config/config.properties"))
  val vNodeNumberPerNode = 3//props.getProperty("vNodeNumberPerNode").toInt
  val commitInterval = 3000//props.getProperty("flushInterval").toInt
}
