package cn.pandadb.costore.config

object globalConfig {
  //  lazy val props = new Properties()
  //  props.load(globalConfig.getClass.getResourceAsStream("../config/config.properties"))
  val vNodeNumberPerNode = 3//props.getProperty("vNodeNumberPerNode").toInt
  val flushInterval = 3//props.getProperty("flushInterval").toInt
}
