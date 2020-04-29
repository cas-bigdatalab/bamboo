import java.util

import cn.pandadb.costore.shard.{NodeRPC, NodeService}
import org.junit.{After, Before, Test}

class ServiceTest1{

  val node = new NodeService("localhost", 11234)

  @Test
  def start(): Unit ={
    node.start()
  }
}