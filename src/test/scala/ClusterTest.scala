import cn.pandadb.costore.cluster.ClusterRPC
import org.junit.{After, Before, Test}

class ClusterTest{
  val client = new ClusterRPC("localhost", 11236)

  @Before
  def buildIndex(): Unit ={
    client.addNode(Map("id" -> "1", "name" -> "blue", "url" -> "talent.com"))
    client.addNode(Map("id" -> "2", "name" -> "excelwang", "url" -> "talent.com"))
    client.addNode(Map("id" -> "3", "name" -> "zhongxin", "url" -> "talent.com"))
  }

  @After
  def clearIndex(): Unit ={
    client.deleteAll()
  }

  @Test
  def search(): Unit ={
    val ret = client.filterNodes(Map(("name" -> "blue")))
    println(ret)
  }

  @Test
  def deleteIndex(): Unit ={
    client.deleteAll()
  }
}