
import cn.pandadb.bamboo.Client
import org.junit.Test

class PerformanceTest {

  val rClient = new Client(List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234"), balancePolicy = "RND", balancePolicyBatch = 30)
  val lClient = new Client(List("localhost:11234", "localhost:11235", "localhost:11236"), balancePolicy = "RND", balancePolicyBatch = 30)

  val batchCNT = 20
  val batchSize = 5

  @Test
  def searchIndex(): Unit = {
    Tool.searchIndex(lClient.filterNodes, batchCNT*batchSize)
  }

  @Test
  def buildIndexSyn(): Unit = {
    lClient.deleteAll()
    Tool.buildIndex(lClient.addNode, batchCNT, batchSize)
  }

  @Test
  def buildIndexAsyn(): Unit = {
    lClient.deleteAll()
    Tool.buildIndex(lClient.addNodeAsyn, batchCNT, batchSize)
  }

  @Test
  def deleteIndex(): Unit = {
    lClient.deleteAll()
  }

  @Test
  def remoteSearchIndex(): Unit = {
    Tool.searchIndex(rClient.filterNodes, batchCNT*batchSize)
  }

  @Test
  def remoteBuildIndexSyn(): Unit = {
    rClient.deleteAll()
    Tool.buildIndex(rClient.addNode, batchCNT, batchSize)
  }

  @Test
  def remoteBuildIndexAsyn(): Unit = {
    rClient.deleteAll()
    Tool.buildIndex(rClient.addNodeAsyn, batchCNT, batchSize)
  }

  @Test
  def remoteDeleteIndex(): Unit = {
    rClient.deleteAll()
  }
}