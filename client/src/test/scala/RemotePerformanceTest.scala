
import cn.pandadb.bamboo.Client
import org.junit.{After, Test}

class RemotePerformanceTest{

  val client = new Client(List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234"), balancePolicy = "RND", balancePolicyBatch = 30)

  // scalastyle:off
  @Test
  def buildIndex(): Unit = {
    client.deleteAll()
    val itersWarmer = 10
    val itersOuter = 1000
    val itersInner = 100
    var start: Long = 0
    (1  to itersOuter + itersWarmer).foreach(oid => {
      if (oid == itersWarmer + 1) {//skip warmer iters
        start = System.currentTimeMillis
      }
      (1  to itersInner).par.foreach(id => {
        val pid = id + itersInner*(oid - 1)
        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
        client.addNode(doc)
      })
    })
    val end = System.currentTimeMillis
    println(s"write ${itersOuter * itersInner / (end - start).toFloat * 1000} nodes per second to bamboo")
  }

  @Test
  def buildIndexAsync(): Unit ={
    val itersWarmer = 10
    val itersOuter = 1000
    val itersInner = 200
    var start: Long = 0
    (1  to itersOuter+itersWarmer).foreach(oid  =>  {
      if (oid==itersWarmer+1){//skip warmer iters
        start = System.currentTimeMillis
      }
      (1  to itersInner).par.foreach(id  =>  {
        val pid = id+itersInner*(oid-1)
        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
        client.addNodeAsyn(doc)
      })
    })
    val end = System.currentTimeMillis
    println(s"write ${itersOuter*itersInner/(end-start).toFloat*1000} nodes per second to bamboo")
  }

  @Test
  def search(): Unit ={
    val itersOuter = 10
    (1 to itersOuter).foreach(oid => {
      val start = System.currentTimeMillis
      val ret = client.filterNodes(Map("name" -> s"bluejoe_${oid*10000}"))
      val end = System.currentTimeMillis
      println(s"search results returned in ${end - start} ms")
      println(ret)
    })
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
  // scalastyle:on
}