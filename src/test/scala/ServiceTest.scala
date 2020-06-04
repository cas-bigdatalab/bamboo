import cn.pandadb.bamboo.NodeService
import org.junit.{After, Test}

class ServiceTest{

  val nodesInfo = List("localhost:11234", "localhost:11235", "localhost:11236")
  val services = nodesInfo.map(n => new NodeService(n, nodesInfo, 3))

  @Test
  def start(): Unit ={
    services.par.foreach(s => s.start())
  }

  @After
  def stop(): Unit ={
    services.par.foreach(s => s.stop())
  }

}