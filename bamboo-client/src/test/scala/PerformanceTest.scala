
import cn.pandadb.bamboo.Client
import org.junit.Test

class PerformanceTest{

  val rClient = new Client(List("10.0.82.216:11234", "10.0.82.217:11234", "10.0.82.218:11234"), balancePolicy = "RND", balancePolicyBatch = 30)
  val lClient = new Client(List("localhost:11234", "localhost:11235", "localhost:11236"), balancePolicy = "RND", balancePolicyBatch = 30)

  @Test
  def searchIndex(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    Tool.searchIndex(lClient.filterNodes, batchCNT*batchSize)
  }

  @Test
  def buildIndexSyn(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    lClient.deleteAll()
    Tool.buildIndex(lClient.addNode, batchCNT, batchSize)
  }

  @Test
  def buildIndexAsyn(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    lClient.deleteAll()
    Tool.buildIndex(lClient.addNodeAsyn, batchCNT, batchSize)
  }

  @Test
  def deleteIndex(): Unit = {
    lClient.deleteAll()
  }

  @Test
  def remoteSearchIndex(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    Tool.searchIndex(rClient.filterNodes, batchCNT*batchSize)
  }

  @Test
  def remoteBuildIndexSyn(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    rClient.deleteAll()
    Tool.buildIndex(rClient.addNode, batchCNT, batchSize)
  }

  @Test
  def remoteBuildIndexAsyn(): Unit = {
    val batchCNT = 200
    val batchSize = 3
    rClient.deleteAll()
    Tool.buildIndex(rClient.addNodeAsyn, batchCNT, batchSize)
  }

  @Test
  def remoteDeleteIndex(): Unit = {
    rClient.deleteAll()
  }
}