import cn.pandadb.costore.{Client, VNode}
import org.junit.{After, Before, Test}

class PerformanceTest{

  val client = new Client(List("localhost:11234"))

  @Test
  def buildIndex(): Unit ={
    val  iters = 1024//*1024
    val start = System.currentTimeMillis
    (1  to iters).foreach(id  =>  {
      client.addNode(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
    })
    val end = System.currentTimeMillis
    println(s"write $iters nodes to costore cost " + (end-start) + " ms")
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
  def writeIndex(): Unit ={
    val vnode = new VNode(10000, )
    val  iters = 50//*1024
    vnode.write()
    val ret = client.filterNodes(Map(("name" -> "bluejoe_1024")))
    println(ret)
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
}