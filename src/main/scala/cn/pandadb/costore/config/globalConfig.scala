package cn.pandadb.costore.config

object globalConfig {
  //  lazy val props = new Properties()
  //  props.load(globalConfig.getClass.getResourceAsStream("../config/config.properties"))
  val replicaFactor = 3//props.getProperty("replicaFactor").toInt
  val vNodeNumberPerNode = 3//props.getProperty("vNodeNumberPerNode").toInt
  val flushInterval = 1//props.getProperty("flushInterval").toInt
}
