import cn.pandadb.costore.node.NodeService
import org.junit.Test

class ServiceTest3{

  val node = new NodeService("localhost", 11236)

  @Test
  def start(): Unit ={
    node.start()
  }

}