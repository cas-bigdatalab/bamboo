import org.apache.logging.log4j.scala.Logging

object Tool extends Logging {
  def buildIndex(indexF: Map[String, String] => Any, batchCNT: Int, batchSize: Int, warmingCNT: Int = 10): Unit = {
    var start: Long = 0
    (1  to batchCNT + warmingCNT).foreach(oid => {
      if (oid == warmingCNT + 1) {//skip warmer iters
        start = System.currentTimeMillis
      }
      (1  to batchSize).par.foreach(id => {
        val pid = id + batchSize*(oid - 1)
        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
        indexF(doc)
      })
    })
    val end = System.currentTimeMillis
    logger.info(s"write ${batchCNT * batchSize / (end - start).toFloat * 1000} nodes per second to bamboo")
  }

  def searchIndex(searchF: Map[String, String] => List[Map[String, String]], totalCNT: Int): Unit = {
    val interval = totalCNT/10
    (1 to 10).foreach(oid => {
      val start = System.currentTimeMillis
      val q = Map("name" -> s"bluejoe_${oid*interval}")
      val ret = searchF(q)
      val end = System.currentTimeMillis
      logger.info(s"search: $q, return: $ret, cost ${end - start} ms")
    })
  }

}
