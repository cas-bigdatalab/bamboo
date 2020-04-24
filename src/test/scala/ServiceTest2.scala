import cn.pandadb.costore.node.NodeService
import org.junit.Test

class ServiceTest2{

  val node = new NodeService("localhost", 11235)

  @Test
  def start(): Unit ={
    node.start()
  }

}