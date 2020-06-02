import cn.pandadb.costore.{Client, VNode}
import org.junit.{After, Before, Test}

class PerformanceTest{

  val client = new Client(List("localhost:11234"))

  @Before
  def buildIndex(): Unit ={
    val  iters = 1024
    val start = System.currentTimeMillis
    (1  to iters).foreach(id  =>  {
      client.addNodeAsyn(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
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
    Thread.sleep(20000)
    val ret = client.filterNodes(Map(("name" -> "bluejoe_1024")))
    println(ret)
  }

  @Test
  def writeIndex(): Unit ={
    val vnode = new VNode("10000")
    (1  to 50).foreach( id =>{
      val start = System.currentTimeMillis
      vnode.write(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
      val end = System.currentTimeMillis
      println(s"write one node to VNode cost " + (end-start) + " ms")
    })
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
}