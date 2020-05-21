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
    val itersOuter = 1000//
    val itersInner = 500//
    val start = System.currentTimeMillis
    (1  to itersOuter).foreach(oid  =>  {
      (1  to itersInner).par.foreach(id  =>  {
        client.addNode(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
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
    val ret = client.filterNodes(Map(("name" -> "bluejoe_1024")))
    println(ret)
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
}