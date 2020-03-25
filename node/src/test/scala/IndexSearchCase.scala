package cn.pandadb.costore.node.test

import cn.pandadb.costore.node.Index
import org.apache.lucene.document.Document
import org.apache.lucene.document.Field
import org.apache.lucene.document.StringField
import org.apache.lucene.document.TextField
import org.apache.lucene.queryparser.classic.QueryParser
import org.junit.{After, Before, Test}
import java.nio.file.Paths
import org.apache.lucene.index.DirectoryReader
import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.store.FSDirectory
import org.apache.lucene.analysis.standard.StandardAnalyzer

class IndexSearchCase{

  val indexDir = "./data"

  def createDocument(id: Int, firstName: String, lastName: String, website: String): Document =
  {
    var document: Document = new Document();
    document.add(new StringField("id", id.toString(), Field.Store.YES));
    document.add(new TextField("firstName", firstName, Field.Store.YES));
    document.add(new TextField("lastName", lastName, Field.Store.YES));
    document.add(new TextField("website", website, Field.Store.YES));
    document;
  }

  @Before
  def buildIndex(): Unit ={
    val writer = Index.createWriter(indexDir)
    writer.addDocument(createDocument(1, "blue", "joe", "talent.com"))
    writer.addDocument(createDocument(2, "excelwang", "wang", "test.com"))
    writer.addDocument(createDocument(3, "zhongxin", "liu", "angle.com"))
    writer.close();
  }

  @After
  def close(): Unit ={
    val writer = Index.createWriter(indexDir)
    writer.deleteAll();
    writer.close();
  }

  @Test
  def count(): Unit ={
    val dir = FSDirectory.open(Paths.get(indexDir))
    val reader = DirectoryReader.open(dir)
    assert(reader.numDocs()==3)
  }

  @Test
  def search(): Unit ={
    val firstName: String = "blue"
    val dir = FSDirectory.open(Paths.get(indexDir))
    val reader = DirectoryReader.open(dir)
    val searcher = new IndexSearcher(reader)
    val qp = new QueryParser("firstName", new StandardAnalyzer)
    val nameQuery = qp.parse(firstName)
    val hits = searcher.search(nameQuery, 10)
    assert(hits.scoreDocs.length!=0)
    hits.scoreDocs.map(d => println(searcher.doc(d.doc).get("firstName")))
  }
}