
import cn.pandadb.bamboo.Client
import org.junit.{After, Before, Test}

class PerformanceTest{

  val client = new Client(List("localhost:11234"))

  // scalastyle:off
  @Before
  def buildIndex(): Unit = {
    val  iters = 1024
    val start = System.currentTimeMillis
    (1 to iters).par.foreach(id => {
      client.addNode(Map("id" -> s"$id", "name" -> s"bluejoe_$id", "url" -> s"talent.com_$id"))
    })
    val end = System.currentTimeMillis

    println(s"write $iters nodes to bamboo cost " + (end-start) + " ms")
  }

//  @After
//  def clearIndex(): Unit ={
//    client.deleteAll()
//  }

  @Test
  def search(): Unit = {
    Thread.sleep(20000)
    val ret = client.filterNodes(Map(("name" -> "bluejoe_1024")))
    println(ret)
  }

  @Test
  def deleteIndex(): Unit = {
    client.deleteAll()
  }
  // scalastyle:on
}