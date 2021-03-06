
import cn.pandadb.bamboo.Client
import org.junit.{After, Before, Test}

class ClientTest {

  val client = new Client(List("localhost:11234"))

  @Before
  def buildIndex(): Unit = {
    client.addNode(Map("id" -> "1", "name" -> "bluejoe", "url" -> "talent.com"))
    client.addNode(Map("id" -> "2", "name" -> "excelwang", "url" -> "talent.com"))
    client.addNode(Map("id" -> "3", "name" -> "zhongxin", "url" -> "talent.com"))
  }

  @After
  def clearIndex(): Unit = {
    client.deleteAll()
  }

  @Test
  def search(): Unit = {
    val ret = client.filterNodes(Map(("name" -> "bluejoe")))
    // scalastyle:off
    println(ret)
    // scalastyle:on
  }

  @Test
  def deleteIndex(): Unit = {
    client.deleteAll()
  }
}