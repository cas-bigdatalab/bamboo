import cn.pandadb.costore.Client
import org.junit.{After, Test}

class RemotePerformanceTest{

  val client = new Client(List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234"), balancePolicy="RND", balancePolicyBatch = 30)

  @Test
  def buildIndex(): Unit ={
//    val time = 60000
//    val stress = 1000
//    val interval = 50
//    val batch = stress/interval
//    val start = System.currentTimeMillis
//    (1  to time/interval).foreach(oid  =>  {
//      (1  to batch).par.foreach(id  =>  {
//        val pid = id+batch*(oid-1)
//        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
//        client.addNode(doc)
//      })
//      Thread.sleep(interval)
//    })
//    val end = System.currentTimeMillis
//    println(s"write ${time/interval*batch/(end-start).toFloat*1000} nodes per second to costore")
//    client.deleteAll()
    val itersWarmer = 10
    val itersOuter = 1000
    val itersInner = 100
    var start: Long = 0
    (1  to itersOuter+itersWarmer).foreach(oid  =>  {
      if (oid==itersWarmer+1){//skip warmer iters
        start = System.currentTimeMillis
      }
      (1  to itersInner).par.foreach(id  =>  {
        val pid = id+itersInner*(oid-1)
        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
        client.addNode(doc)
        Thread.sleep(2)
      })
    })
    val end = System.currentTimeMillis
    println(s"write ${itersOuter*itersInner/(end-start).toFloat*1000} nodes per second to costore")
  }

  @Test
  def search(): Unit ={
    val itersOuter = 100
    (1 to itersOuter).foreach(oid => {
      val start = System.currentTimeMillis
      val ret = client.filterNodes(Map("name" -> s"bluejoe_$oid"))
      val end = System.currentTimeMillis
      println(s"search results returned in ${end - start} ms")
      println(ret)
    })
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
}