import cn.pandadb.costore.Client
import org.junit.Test

class RemotePerformanceTest{

  val client = new Client(List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234"))

  @Test
  def buildIndex(): Unit ={
    val  warmIters = 100//*1024
    (1  to warmIters).foreach(id  =>  {
      client.addNode(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
    })
    client.deleteAll()
    val itersOuter = 500//
    val itersInner = 1000//
    val start = System.currentTimeMillis
    (1  to itersOuter).foreach(oid  =>  {
      (1  to itersInner).par.foreach(id  =>  {
        val pid = id+itersInner*(oid-1)
        val doc = Map("id" -> s"$pid", "name" -> s"bluejoe_$pid", "url" -> s"talent.com_$pid")
        client.addNode(doc)
      })
      Thread.sleep(10)
    })
    val end = System.currentTimeMillis
    println(s"write ${itersOuter*itersInner/(end-start).toFloat*1000} nodes per second to costore")
  }

//  @After
//  def clearIndex(): Unit ={
//    client.deleteAll()
//  }

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