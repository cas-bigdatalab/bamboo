import cn.pandadb.costore.NodeService
import cn.pandadb.costore.config.globalConfig
import org.junit.{After, Test}

class ServiceTest{

  val services = globalConfig.nodesInfo.map(n => new NodeService(n))

  @Test
  def start(): Unit ={
    services.par.foreach(s => s.start())
  }

  @After
  def stop(): Unit ={
    services.par.foreach(s => s.stop())
  }

}