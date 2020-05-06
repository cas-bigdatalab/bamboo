import cn.pandadb.costore.NodeService
import cn.pandadb.costore.config.globalConfig.nodes
import org.junit.{After, Test}

class ServiceTest{

  val services = nodes.map(n => new NodeService(n))

  @Test
  def start(): Unit ={
    services.par.foreach(s => s.start())
  }

  @After
  def stop(): Unit ={
    services.par.foreach(s => s.stop())
  }

}