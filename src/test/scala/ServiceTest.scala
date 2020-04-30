import cn.pandadb.costore.config.globalConfig.nodes
import org.junit.{After, Test}

class ServiceTest{

  @Test
  def start(): Unit ={
    nodes.par.foreach(n => n.start())
  }

//  @After
//  def stop(): Unit ={
//    nodes.par.foreach(n => n.stop())
//  }

}