import cn.pandadb.costore.{Client, NodeRpc}
import org.junit.{After, Before, Test}

class ClientTest{

  val client = new Client("localhost:11234")

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